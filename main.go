// ClipLink —— 局域网剪切板助手
// 单文件服务端：无第三方依赖，提供 HTTP API、SSE 实时同步与本地历史持久化。
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed web
var webFS embed.FS

const (
	maxTextSize = 256 << 10 // 单条内容上限 256 KB
	version     = "1.3.0"
)

type Item struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Source    string    `json:"source"`
	IP        string    `json:"ip"`
	Pinned    bool      `json:"pinned"`
	CreatedAt time.Time `json:"createdAt"`
}

type sseMessage struct {
	Event string
	Data  string
}

// Hub 维护所有 SSE 连接，用于把变更实时推送到所有设备。
type Hub struct {
	mu      sync.Mutex
	clients map[chan sseMessage]struct{}
}

func newHub() *Hub {
	return &Hub{clients: make(map[chan sseMessage]struct{})}
}

func (h *Hub) subscribe() chan sseMessage {
	ch := make(chan sseMessage, 8)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) unsubscribe(ch chan sseMessage) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

func (h *Hub) broadcast(event string, data any) {
	payload, err := json.Marshal(data)
	if err != nil {
		return
	}
	msg := sseMessage{Event: event, Data: string(payload)}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default: // 客户端消费过慢时丢弃本次通知，前端还有轮询兜底
		}
	}
}

// Store 负责内存中的历史记录与磁盘持久化（JSON Lines）。
type Store struct {
	mu    sync.RWMutex
	items []Item
	file  string
	max   int
}

func (s *Store) load() {
	f, err := os.Open(s.file)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		log.Printf("读取历史记录失败: %v", err)
		return
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 64<<10)
	for {
		line, err := reader.ReadString('\n')
		if line = strings.TrimSpace(line); line != "" {
			var it Item
			if json.Unmarshal([]byte(line), &it) == nil && it.ID != "" {
				s.items = append(s.items, it)
			}
		}
		if err != nil {
			break
		}
	}
	sort.SliceStable(s.items, func(i, j int) bool {
		return s.items[i].CreatedAt.After(s.items[j].CreatedAt)
	})
	s.mu.Lock()
	s.trimLocked()
	s.mu.Unlock()
}

// trimLocked 仅裁剪未收藏的旧记录，收藏项永久保留。
func (s *Store) trimLocked() {
	kept := make([]Item, 0, len(s.items))
	unpinned := 0
	for _, it := range s.items {
		if it.Pinned {
			kept = append(kept, it)
			continue
		}
		unpinned++
		if unpinned <= s.max {
			kept = append(kept, it)
		}
	}
	s.items = kept
}

func (s *Store) persistLocked() {
	if err := os.MkdirAll(filepath.Dir(s.file), 0o755); err != nil {
		log.Printf("创建数据目录失败: %v", err)
		return
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	for _, it := range s.items {
		_ = enc.Encode(it)
	}
	tmp := s.file + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		log.Printf("写入历史记录失败: %v", err)
		return
	}
	if err := os.Rename(tmp, s.file); err != nil {
		log.Printf("保存历史记录失败: %v", err)
	}
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func (s *Store) Add(text, source, ip string) (Item, error) {
	if strings.TrimSpace(text) == "" {
		return Item{}, errors.New("内容不能为空")
	}
	if len(text) > maxTextSize {
		return Item{}, fmt.Errorf("内容过长（上限 %d KB）", maxTextSize/1024)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	// 相同内容视为同一份记忆：移到最前并刷新时间，避免重复堆积。
	for i := range s.items {
		if s.items[i].Text == text {
			it := s.items[i]
			it.CreatedAt = now
			if source != "" {
				it.Source = source
			}
			if ip != "" {
				it.IP = ip
			}
			s.items = append(s.items[:i], s.items[i+1:]...)
			s.items = append([]Item{it}, s.items...)
			s.persistLocked()
			return it, nil
		}
	}

	it := Item{ID: newID(), Text: text, Source: source, IP: ip, CreatedAt: now}
	s.items = append([]Item{it}, s.items...)
	s.trimLocked()
	s.persistLocked()
	return it, nil
}

func (s *Store) List(query, filter string) ([]Item, map[string]int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	q := strings.ToLower(strings.TrimSpace(query))
	counts := map[string]int{"all": len(s.items), "pinned": 0}
	for _, it := range s.items {
		if it.Pinned {
			counts["pinned"]++
		}
	}

	out := make([]Item, 0, len(s.items))
	for _, it := range s.items {
		if filter == "pinned" && !it.Pinned {
			continue
		}
		if q != "" &&
			!strings.Contains(strings.ToLower(it.Text), q) &&
			!strings.Contains(strings.ToLower(it.Source), q) {
			continue
		}
		out = append(out, it)
	}
	return out, counts
}

func (s *Store) SetPinned(id string, pinned bool) (Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		if s.items[i].ID == id {
			s.items[i].Pinned = pinned
			s.persistLocked()
			return s.items[i], true
		}
	}
	return Item{}, false
}

func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		if s.items[i].ID == id {
			s.items = append(s.items[:i], s.items[i+1:]...)
			s.persistLocked()
			return true
		}
	}
	return false
}

// Batch 批量收藏、取消收藏或删除记录，返回实际变更条数。
func (s *Store) Batch(ids []string, action string) int {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	affected := 0
	switch action {
	case "delete":
		kept := make([]Item, 0, len(s.items))
		for _, it := range s.items {
			if _, ok := set[it.ID]; ok {
				affected++
				continue
			}
			kept = append(kept, it)
		}
		if affected > 0 {
			s.items = kept
			s.persistLocked()
		}
	case "pin", "unpin":
		pinned := action == "pin"
		for i := range s.items {
			if _, ok := set[s.items[i].ID]; ok && s.items[i].Pinned != pinned {
				s.items[i].Pinned = pinned
				affected++
			}
		}
		if affected > 0 {
			s.persistLocked()
		}
	}
	return affected
}

// Clear 删除未收藏记录；all=true 时清空全部（包含收藏）。
func (s *Store) Clear(all bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := make([]Item, 0, len(s.items))
	removed := 0
	for _, it := range s.items {
		if all || !it.Pinned {
			removed++
			continue
		}
		kept = append(kept, it)
	}
	if removed > 0 {
		s.items = kept
		s.persistLocked()
	}
	return removed
}

// FileMeta 描述一份共享文件的元数据，文件本体保存在磁盘上的 files 目录。
type FileMeta struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	Mime      string    `json:"mime"`
	Source    string    `json:"source"`
	IP        string    `json:"ip"`
	CreatedAt time.Time `json:"createdAt"`
}

type FileStore struct {
	mu       sync.RWMutex
	files    []FileMeta
	dir      string
	metaFile string
	maxSize  int64
}

func (s *FileStore) load() {
	f, err := os.Open(s.metaFile)
	if err == nil {
		reader := bufio.NewReaderSize(f, 64<<10)
		for {
			line, readErr := reader.ReadString('\n')
			if line = strings.TrimSpace(line); line != "" {
				var meta FileMeta
				if json.Unmarshal([]byte(line), &meta) == nil && meta.ID != "" {
					if _, statErr := os.Stat(s.path(meta.Name, meta.ID)); statErr == nil {
						s.files = append(s.files, meta)
					}
				}
			}
			if readErr != nil {
				break
			}
		}
		f.Close()
	}
	sort.SliceStable(s.files, func(i, j int) bool {
		return s.files[i].CreatedAt.After(s.files[j].CreatedAt)
	})
}

func (s *FileStore) path(name, id string) string {
	return filepath.Join(s.dir, id+safeExt(name))
}

func (s *FileStore) persistLocked() {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	for _, meta := range s.files {
		_ = enc.Encode(meta)
	}
	tmp := s.metaFile + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		log.Printf("写入文件索引失败: %v", err)
		return
	}
	if err := os.Rename(tmp, s.metaFile); err != nil {
		log.Printf("保存文件索引失败: %v", err)
	}
}

func (s *FileStore) List() ([]FileMeta, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]FileMeta, len(s.files))
	copy(out, s.files)
	var total int64
	for _, meta := range out {
		total += meta.Size
	}
	return out, total
}

func (s *FileStore) Add(meta FileMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files = append([]FileMeta{meta}, s.files...)
	s.persistLocked()
}

func (s *FileStore) Get(id string) (FileMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, meta := range s.files {
		if meta.ID == id {
			return meta, true
		}
	}
	return FileMeta{}, false
}

func (s *FileStore) Delete(id string) (FileMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, meta := range s.files {
		if meta.ID != id {
			continue
		}
		s.files = append(s.files[:i], s.files[i+1:]...)
		_ = os.Remove(s.path(meta.Name, meta.ID))
		s.persistLocked()
		return meta, true
	}
	return FileMeta{}, false
}

func sanitizeFilename(name string) string {
	name = strings.ReplaceAll(name, "\\", "/")
	name = filepath.Base(name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return "未命名文件"
	}
	if r := []rune(name); len(r) > 160 {
		name = string(r[:160])
	}
	return name
}

// safeExt 只保留字母数字扩展名，避免写入磁盘时出现异常路径。
func safeExt(name string) string {
	ext := strings.ToLower(filepath.Ext(name))
	if len(ext) < 2 || len(ext) > 12 {
		return ""
	}
	for _, c := range ext[1:] {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
			return ""
		}
	}
	return ext
}

// canInlineMime 限制可内联预览的类型，SVG 等可能携带脚本的类型一律强制下载。
func canInlineMime(m string) bool {
	m = strings.ToLower(strings.TrimSpace(strings.Split(m, ";")[0]))
	if m == "image/svg+xml" {
		return false
	}
	return strings.HasPrefix(m, "image/") || strings.HasPrefix(m, "video/") || strings.HasPrefix(m, "audio/")
}

type Server struct {
	store    *Store
	files    *FileStore
	hub      *Hub
	addr     string
	mdnsHost string
	stop     chan struct{}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	page, err := webFS.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, "页面资源缺失", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(page)
}

// 站点图标等静态资源的 MIME 类型。
var webAssetTypes = map[string]string{
	"icon.svg":             "image/svg+xml",
	"favicon.ico":          "image/x-icon",
	"icon-192.png":         "image/png",
	"icon-512.png":         "image/png",
	"apple-touch-icon.png": "image/png",
	"manifest.webmanifest": "application/manifest+json",
}

func (s *Server) handleWebAsset(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	ctype, ok := webAssetTypes[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	data, err := webFS.ReadFile("web/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", ctype)
	if name == "manifest.webmanifest" {
		w.Header().Set("Cache-Control", "no-cache")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=86400")
	}
	_, _ = w.Write(data)
}

// handleShutdown 允许本机页面退出客户端，局域网其他设备无法调用。
func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "不支持的方法")
		return
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		writeErr(w, http.StatusForbidden, "仅允许本机操作")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	select {
	case s.stop <- struct{}{}:
	default:
	}
}

// handleQRCode 提供离线二维码生成脚本（qrcode-generator, MIT）。
func (s *Server) handleQRCode(w http.ResponseWriter, r *http.Request) {
	data, err := webFS.ReadFile("web/qrcode.min.js")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(data)
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	urls, host := lanURLs(s.addr)
	payload := map[string]any{
		"ok":      true,
		"version": version,
		"host":    host,
		"urls":    urls,
	}
	if s.mdnsHost != "" {
		payload["name"] = s.mdnsHost
		payload["nameUrl"] = "http://" + s.mdnsHost + ":" + listenPort(s.addr)
	}
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) handleItems(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		items, counts := s.store.List(r.URL.Query().Get("q"), r.URL.Query().Get("filter"))
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "counts": counts})

	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, maxTextSize*2)
		var body struct {
			Text   string `json:"text"`
			Source string `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "请求格式错误")
			return
		}
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		item, err := s.store.Add(body.Text, strings.TrimSpace(body.Source), ip)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		s.hub.broadcast("sync", map[string]any{"action": "add", "id": item.ID})
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "item": item})

	case http.MethodDelete:
		removed := s.store.Clear(r.URL.Query().Get("all") == "1")
		s.hub.broadcast("sync", map[string]any{"action": "clear", "removed": removed})
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "removed": removed})

	default:
		writeErr(w, http.StatusMethodNotAllowed, "不支持的方法")
	}
}

// handleBatchItems 批量收藏/取消收藏/删除记录。
func (s *Server) handleBatchItems(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "不支持的方法")
		return
	}
	var body struct {
		Action string   `json:"action"`
		IDs    []string `json:"ids"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求格式错误")
		return
	}
	if body.Action != "pin" && body.Action != "unpin" && body.Action != "delete" {
		writeErr(w, http.StatusBadRequest, "不支持的批量操作")
		return
	}
	if len(body.IDs) == 0 || len(body.IDs) > 500 {
		writeErr(w, http.StatusBadRequest, "请选择 1-500 条记录")
		return
	}
	affected := s.store.Batch(body.IDs, body.Action)
	if affected > 0 {
		s.hub.broadcast("sync", map[string]any{"action": "batch", "op": body.Action})
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "affected": affected})
}

func (s *Server) handleItem(w http.ResponseWriter, r *http.Request) {
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/items/"), "/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusNotFound, "记录不存在")
		return
	}
	id := parts[0]

	switch {
	case r.Method == http.MethodPost && len(parts) == 2 && parts[1] == "pin":
		var body struct {
			Pinned bool `json:"pinned"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "请求格式错误")
			return
		}
		item, ok := s.store.SetPinned(id, body.Pinned)
		if !ok {
			writeErr(w, http.StatusNotFound, "记录不存在")
			return
		}
		s.hub.broadcast("sync", map[string]any{"action": "pin", "id": id})
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "item": item})

	case r.Method == http.MethodDelete && len(parts) == 1:
		if !s.store.Delete(id) {
			writeErr(w, http.StatusNotFound, "记录不存在")
			return
		}
		s.hub.broadcast("sync", map[string]any{"action": "delete", "id": id})
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})

	default:
		writeErr(w, http.StatusMethodNotAllowed, "不支持的方法")
	}
}

func (s *Server) handleFiles(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		files, total := s.files.List()
		writeJSON(w, http.StatusOK, map[string]any{"files": files, "totalSize": total})
	case http.MethodPost:
		s.uploadFile(w, r)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "不支持的方法")
	}
}

func (s *Server) uploadFile(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.files.maxSize+(1<<20))
	reader, err := r.MultipartReader()
	if err != nil {
		writeErr(w, http.StatusBadRequest, "请使用 multipart/form-data 上传")
		return
	}

	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	source := ""
	var (
		fileID, fileName, filePath, fileMime string
		fileSize                             int64
	)

	tooLarge := func(err error) bool {
		var maxErr *http.MaxBytesError
		return errors.As(err, &maxErr)
	}

	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if filePath != "" {
				_ = os.Remove(filePath)
			}
			if tooLarge(err) {
				writeErr(w, http.StatusRequestEntityTooLarge,
					fmt.Sprintf("文件过大（上限 %d MB）", s.files.maxSize>>20))
				return
			}
			writeErr(w, http.StatusBadRequest, "上传中断")
			return
		}

		switch part.FormName() {
		case "source":
			buf, _ := io.ReadAll(io.LimitReader(part, 512))
			source = strings.TrimSpace(string(buf))
			_ = part.Close()
		case "file":
			if part.FileName() == "" || fileID != "" {
				_, _ = io.Copy(io.Discard, part)
				_ = part.Close()
				continue
			}
			name := sanitizeFilename(part.FileName())
			id := newID()
			path := filepath.Join(s.files.dir, id+safeExt(name))
			dst, createErr := os.Create(path)
			if createErr != nil {
				_ = part.Close()
				writeErr(w, http.StatusInternalServerError, "无法保存文件")
				return
			}
			size, copyErr := io.Copy(dst, part)
			_ = dst.Close()
			_ = part.Close()
			if copyErr != nil {
				_ = os.Remove(path)
				if tooLarge(copyErr) {
					writeErr(w, http.StatusRequestEntityTooLarge,
						fmt.Sprintf("文件过大（上限 %d MB）", s.files.maxSize>>20))
					return
				}
				writeErr(w, http.StatusBadRequest, "上传中断")
				return
			}
			fileID, fileName, filePath, fileSize = id, name, path, size
			fileMime = part.Header.Get("Content-Type")
			if fileMime == "" || strings.EqualFold(fileMime, "application/octet-stream") {
				if detected := mime.TypeByExtension(filepath.Ext(name)); detected != "" {
					fileMime = detected
				}
			}
			if fileMime == "" {
				fileMime = "application/octet-stream"
			}
		default:
			_, _ = io.Copy(io.Discard, part)
			_ = part.Close()
		}
	}

	if fileID == "" || filePath == "" {
		writeErr(w, http.StatusBadRequest, "没有收到文件")
		return
	}

	meta := FileMeta{
		ID:        fileID,
		Name:      fileName,
		Size:      fileSize,
		Mime:      fileMime,
		Source:    source,
		IP:        ip,
		CreatedAt: time.Now(),
	}
	s.files.Add(meta)
	s.hub.broadcast("files", map[string]any{"action": "add", "id": meta.ID, "name": meta.Name})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "file": meta})
}

func (s *Server) handleFile(w http.ResponseWriter, r *http.Request) {
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/files/"), "/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusNotFound, "文件不存在")
		return
	}
	id := parts[0]

	if r.Method == http.MethodDelete && len(parts) == 1 {
		meta, ok := s.files.Delete(id)
		if !ok {
			writeErr(w, http.StatusNotFound, "文件不存在")
			return
		}
		s.hub.broadcast("files", map[string]any{"action": "delete", "id": id, "name": meta.Name})
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "不支持的方法")
		return
	}

	meta, ok := s.files.Get(id)
	if !ok {
		writeErr(w, http.StatusNotFound, "文件不存在")
		return
	}
	f, err := os.Open(s.files.path(meta.Name, meta.ID))
	if err != nil {
		writeErr(w, http.StatusNotFound, "文件已不存在")
		return
	}
	defer f.Close()

	inline := len(parts) == 2 && parts[1] == "raw" && canInlineMime(meta.Mime)
	contentType := meta.Mime
	disposition := "attachment"
	if inline {
		contentType = meta.Mime
		disposition = "inline"
	} else {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": meta.Name}))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, meta.Name, meta.CreatedAt, f)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := s.hub.subscribe()
	defer s.hub.unsubscribe(ch)

	_, _ = fmt.Fprint(w, "event: hello\ndata: {\"ok\":true}\n\n")
	flusher.Flush()

	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.Event, msg.Data)
			flusher.Flush()
		case <-ping.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// lanIPs 返回本机所有可用于局域网访问的 IPv4 地址。
func lanIPs() []net.IP {
	var ips []net.IP
	ifaces, err := net.Interfaces()
	if err != nil {
		return ips
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || ip.To4() == nil {
				continue
			}
			ips = append(ips, ip.To4())
		}
	}
	return ips
}

func listenPort(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 && i < len(addr)-1 {
		return addr[i+1:]
	}
	return "17800"
}

// lanURLs 返回本机所有可用于局域网访问的地址。
func lanURLs(addr string) ([]string, string) {
	port := listenPort(addr)
	host, _ := os.Hostname()
	var urls []string
	for _, ip := range lanIPs() {
		urls = append(urls, fmt.Sprintf("http://%s:%s", ip.String(), port))
	}
	return urls, host
}

/* ---------- 内置 mDNS 响应器：提供 cliplink.local 短地址 ---------- */

const (
	mdnsGroup = "224.0.0.251"
	mdnsPort  = 5353
)

type mdnsResponder struct {
	name      string
	conn      *net.UDPConn
	closed    chan struct{}
	closeOnce sync.Once
}

func startMDNS(suffix string) (*mdnsResponder, error) {
	suffix = strings.Trim(strings.ToLower(strings.TrimSpace(suffix)), ".")
	suffix = strings.TrimSuffix(suffix, ".local")
	if suffix == "" {
		return nil, errors.New("主机名为空")
	}
	name := suffix + ".local"

	lc := mdnsListenConfig()
	pc, err := lc.ListenPacket(context.Background(), "udp4", fmt.Sprintf("0.0.0.0:%d", mdnsPort))
	if err != nil {
		return nil, err
	}
	conn, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, errors.New("UDP 连接类型异常")
	}

	ifaces, _ := net.Interfaces()
	for i := range ifaces {
		iface := &ifaces[i]
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagMulticast == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok || ipnet.IP.To4() == nil {
				continue
			}
			_ = joinMDNSGroup(conn, ipnet.IP.To4())
			break
		}
	}
	_ = joinMDNSGroup(conn, nil) // 默认接口兜底

	r := &mdnsResponder{name: name, conn: conn, closed: make(chan struct{})}
	go r.serve()
	go r.announce()
	return r, nil
}

func (r *mdnsResponder) serve() {
	buf := make([]byte, 1500)
	group := &net.UDPAddr{IP: net.ParseIP(mdnsGroup), Port: mdnsPort}
	for {
		n, src, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < 12 || !mdnsWantsName(buf[:n], r.name) {
			continue
		}
		packet := buildMDNSAnswer(r.name, lanIPs())
		_, _ = r.conn.WriteToUDP(packet, group)
		if src != nil && src.Port != mdnsPort {
			_, _ = r.conn.WriteToUDP(packet, src)
		}
	}
}

func (r *mdnsResponder) announce() {
	send := func() {
		packet := buildMDNSAnswer(r.name, lanIPs())
		_, _ = r.conn.WriteToUDP(packet, &net.UDPAddr{IP: net.ParseIP(mdnsGroup), Port: mdnsPort})
	}
	send()
	for i := 0; i < 2; i++ {
		select {
		case <-time.After(time.Second):
			send()
		case <-r.closed:
			return
		}
	}
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			send()
		case <-r.closed:
			return
		}
	}
}

func (r *mdnsResponder) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	return r.conn.Close()
}

func buildMDNSAnswer(name string, ips []net.IP) []byte {
	var valid []net.IP
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			valid = append(valid, v4)
		}
	}
	var b bytes.Buffer
	header := make([]byte, 12)
	binary.BigEndian.PutUint16(header[2:4], 0x8400) // 响应 + 权威
	binary.BigEndian.PutUint16(header[6:8], uint16(len(valid)))
	b.Write(header)
	qname := encodeDNSName(name)
	for _, ip := range valid {
		b.Write(qname)
		rr := make([]byte, 10)
		binary.BigEndian.PutUint16(rr[0:2], 1)      // TYPE A
		binary.BigEndian.PutUint16(rr[2:4], 0x8001) // CLASS IN + cache-flush
		binary.BigEndian.PutUint32(rr[4:8], 120)    // TTL
		binary.BigEndian.PutUint16(rr[8:10], 4)     // RDLENGTH
		b.Write(rr)
		b.Write(ip)
	}
	return b.Bytes()
}

func encodeDNSName(name string) []byte {
	name = strings.TrimSuffix(name, ".")
	var out []byte
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 {
			continue
		}
		out = append(out, byte(len(label)))
		out = append(out, label...)
	}
	return append(out, 0)
}

func mdnsWantsName(pkt []byte, name string) bool {
	if len(pkt) < 12 {
		return false
	}
	if binary.BigEndian.Uint16(pkt[2:4])&0x8000 != 0 { // 忽略响应包
		return false
	}
	questions := int(binary.BigEndian.Uint16(pkt[4:6]))
	off := 12
	target := strings.ToLower(name) + "."
	for i := 0; i < questions; i++ {
		qname, ok := readDNSName(pkt, &off)
		if !ok || off+4 > len(pkt) {
			return false
		}
		qtype := binary.BigEndian.Uint16(pkt[off : off+2])
		off += 4
		if strings.EqualFold(qname, target) && (qtype == 1 || qtype == 255) {
			return true
		}
	}
	return false
}

func readDNSName(msg []byte, off *int) (string, bool) {
	var labels []string
	pos := *off
	jumped := false
	for jumps := 0; jumps < 8; jumps++ {
		if pos >= len(msg) {
			return "", false
		}
		length := int(msg[pos])
		if length == 0 {
			pos++
			if !jumped {
				*off = pos
			}
			return strings.Join(labels, ".") + ".", true
		}
		if length&0xC0 == 0xC0 {
			if pos+1 >= len(msg) {
				return "", false
			}
			ptr := int(binary.BigEndian.Uint16(msg[pos:pos+2]) & 0x3FFF)
			if !jumped {
				*off = pos + 2
				jumped = true
			}
			pos = ptr
			continue
		}
		if length&0xC0 != 0 || pos+1+length > len(msg) {
			return "", false
		}
		labels = append(labels, string(msg[pos+1:pos+1+length]))
		pos += 1 + length
	}
	return "", false
}

func main() {
	addr := flag.String("addr", ":17800", "监听地址")
	dataDir := flag.String("data", "", "数据目录（默认用户配置目录；当前目录已有 data 时沿用）")
	keep := flag.Int("keep", 1000, "最多保留的未收藏记录条数")
	maxUpload := flag.Int("maxupload", 2048, "单文件上传大小上限（MB）")
	mdnsName := flag.String("mdns", "cliplink", "mDNS 短主机名（留空禁用）")
	openUI := flag.Bool("open", true, "启动后自动打开界面")
	flag.Parse()

	dir := resolveDataDir(*dataDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatalf("创建数据目录失败: %v", err)
	}
	if logFile, err := os.OpenFile(filepath.Join(dir, "cliplink.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		defer logFile.Close()
		log.SetOutput(io.MultiWriter(logFile, os.Stderr))
	}

	hub := newHub()
	store := &Store{
		file: filepath.Join(dir, "history.jsonl"),
		max:  *keep,
	}
	store.load()

	files := &FileStore{
		dir:      filepath.Join(dir, "files"),
		metaFile: filepath.Join(dir, "files.jsonl"),
		maxSize:  int64(*maxUpload) << 20,
	}
	if err := os.MkdirAll(files.dir, 0o755); err != nil {
		log.Fatalf("创建文件目录失败: %v", err)
	}
	files.load()

	var mdnsHost string
	if *mdnsName != "" {
		if responder, err := startMDNS(*mdnsName); err != nil {
			log.Printf("短主机名未启用: %v", err)
		} else {
			mdnsHost = responder.name
			defer responder.Close()
		}
	}

	stopCh := make(chan struct{}, 1)
	server := &Server{store: store, files: files, hub: hub, addr: *addr, mdnsHost: mdnsHost, stop: stopCh}

	mux := http.NewServeMux()
	mux.HandleFunc("/", server.handleIndex)
	mux.HandleFunc("/api/info", server.handleInfo)
	mux.HandleFunc("/api/shutdown", server.handleShutdown)
	mux.HandleFunc("/qrcode.js", server.handleQRCode)
	mux.HandleFunc("/api/items", server.handleItems)
	mux.HandleFunc("/api/items/batch", server.handleBatchItems)
	mux.HandleFunc("/api/items/", server.handleItem)
	mux.HandleFunc("/api/files", server.handleFiles)
	mux.HandleFunc("/api/files/", server.handleFile)
	mux.HandleFunc("/events", server.handleEvents)
	for name := range webAssetTypes {
		mux.HandleFunc("/"+name, server.handleWebAsset)
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		local := localURL(*addr)
		if *openUI && probeServer(local) {
			log.Printf("ClipLink 已在运行，打开界面: %s", local)
			openBrowser(local)
			return
		}
		log.Fatalf("服务启动失败: %v", err)
	}

	urls, host := lanURLs(*addr)
	log.Printf("ClipLink %s 已启动（%s）", version, host)
	log.Printf("本机访问: %s", localURL(*addr))
	for _, u := range urls {
		log.Printf("局域网访问: %s", u)
	}
	if mdnsHost != "" {
		log.Printf("短主机名: http://%s:%s", mdnsHost, listenPort(*addr))
	}
	log.Printf("历史记录: %s（最多保留 %d 条未收藏）", store.file, *keep)
	log.Printf("共享文件: %s（单文件上限 %d MB）", files.dir, *maxUpload)
	log.Printf("数据目录: %s", dir)

	if *openUI {
		openBrowser(localURL(*addr))
	}

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("服务退出: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-stop:
	case <-stopCh:
	}
	log.Println("正在关闭服务…")
	_ = srv.Close()
}

// resolveDataDir 为双击启动的客户端选择稳定的数据目录。
func resolveDataDir(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if st, err := os.Stat(filepath.Join("data", "history.jsonl")); err == nil && !st.IsDir() {
		return "data"
	}
	if dir, err := os.UserConfigDir(); err == nil {
		newDir := filepath.Join(dir, "ClipLink")
		oldDir := filepath.Join(dir, "CutBank")
		if _, err := os.Stat(newDir); os.IsNotExist(err) {
			if _, err := os.Stat(oldDir); err == nil {
				_ = os.Rename(oldDir, newDir)
			}
		}
		return newDir
	}
	return "data"
}

func localURL(addr string) string {
	return "http://localhost:" + listenPort(addr)
}

func probeServer(url string) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url + "/api/info")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
	return resp.StatusCode == http.StatusOK
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		log.Printf("无法自动打开界面: %v", err)
	}
}
