package handles

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"image"
	"io"
	"net/url"
	"os"
	"os/exec"
	stdpath "path"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

func getLastModified(c *gin.Context) time.Time {
	now := time.Now()
	lastModifiedStr := c.GetHeader("Last-Modified")
	lastModifiedMillisecond, err := strconv.ParseInt(lastModifiedStr, 10, 64)
	if err != nil {
		return now
	}
	lastModified := time.UnixMilli(lastModifiedMillisecond)
	return lastModified
}

func checkFileExists(ctx context.Context, path string) (bool, error) {
	// 使用项目中的文件系统接口检查文件是否存在
	// 注意：根据实际项目中的接口调整
	obj, err := fs.Get(ctx, path, &fs.GetArgs{NoLog: true})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil // 文件不存在
		}
		return false, err // 其他错误
	}

	return obj != nil, nil // 文件存在
}

func FsStream(c *gin.Context) {
	defer func() {
		if n, _ := io.ReadFull(c.Request.Body, []byte{0}); n == 1 {
			_, _ = utils.CopyWithBuffer(io.Discard, c.Request.Body)
		}
		_ = c.Request.Body.Close()
	}()

	// 获取文件路径并处理
	path := c.GetHeader("File-Path")
	path, err := url.PathUnescape(path)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	asTask := c.GetHeader("As-Task") == "true"
	overwrite := c.GetHeader("Overwrite") != "false"
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	path, err = user.JoinPath(path)
	if err != nil {
		common.ErrorResp(c, err, 403)
		return
	}

	if !overwrite {
		if res, _ := fs.Get(c.Request.Context(), path, &fs.GetArgs{NoLog: true}); res != nil {
			common.ErrorStrResp(c, "file exists", 403)
			return
		}
	}

	// 解析文件信息
	dir, name := stdpath.Split(path)
	sizeStr := c.GetHeader("Content-Length")
	if sizeStr == "" {
		sizeStr = "0"
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	// 处理文件哈希信息
	h := make(map[*utils.HashType]string)
	if md5 := c.GetHeader("X-File-Md5"); md5 != "" {
		h[utils.MD5] = md5
	}
	if sha1 := c.GetHeader("X-File-Sha1"); sha1 != "" {
		h[utils.SHA1] = sha1
	}
	if sha256 := c.GetHeader("X-File-Sha256"); sha256 != "" {
		h[utils.SHA256] = sha256
	}

	// 设置MIME类型
	mimetype := c.GetHeader("Content-Type")
	if len(mimetype) == 0 {
		mimetype = utils.GetMimeType(name)
	}

	// 创建文件流对象
	s := &stream.FileStream{
		Obj: &model.Object{
			Name:     name,
			Size:     size,
			Modified: getLastModified(c),
			HashInfo: utils.NewHashInfoByMap(h),
		},
		Reader:       c.Request.Body,
		Mimetype:     mimetype,
		WebPutAsTask: asTask,
	}

	// 执行文件上传
	var t task.TaskExtensionInfo
	if asTask {
		t, err = fs.PutAsTask(c.Request.Context(), dir, s)
	} else {
		err = fs.PutDirectly(c.Request.Context(), dir, s, true)
	}

	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}

	// 异步处理视频缩略图
	if strings.HasPrefix(mimetype, "video/") {
		// 使用独立上下文，避免HTTP请求结束后取消任务
		go generateVideoThumbnail(context.Background(), path, user)
	}

	// 返回结果
	if t == nil {
		common.SuccessResp(c)
		return
	}

	common.SuccessResp(c, gin.H{
		"task": getTaskInfo(t),
	})
}

// 生成视频缩略图（WebP格式）
func generateVideoThumbnail(ctx context.Context, filePath string, user *model.User) {

	// 获取视频文件绝对路径
	fileObj, err := fs.Get(ctx, filePath, &fs.GetArgs{NoLog: true})
	if err != nil {
		logrus.Printf("获取视频文件信息失败: %v", err)
		return
	}

	videoAbsPath := fileObj.GetPath()
	if videoAbsPath == "" {
		logrus.Printf("视频文件绝对路径为空")
		return
	}

	// 解析目标路径
	dir, name := stdpath.Split(filePath)
	targetThumbDir := stdpath.Join(dir, ".thumbnails")
	baseName := strings.TrimSuffix(name, stdpath.Ext(name))
	targetThumbName := baseName + ".webp"
	targetThumbPath := stdpath.Join(targetThumbDir, targetThumbName)

	// 新增：检查目标缩略图是否已存在
	exists, err := checkFileExists(ctx, targetThumbPath)
	if err != nil {
		logrus.Printf("检查缩略图存在性失败: %v", err)
		return
	}
	if exists {
		logrus.Printf("缩略图已存在，跳过生成: %s", targetThumbPath)
		return
	}
	targetThumbName = baseName + ".webp"
	targetThumbPath = stdpath.Join(targetThumbDir, targetThumbName)

	// 创建本地临时文件（修改：使用.webp扩展名）
	tempFile, err := os.CreateTemp(os.TempDir(), "video_thumb_*.webp")
	if err != nil {
		logrus.Printf("创建本地临时文件失败: %v", err)
		return
	}

	tempFilePath := tempFile.Name()
	_ = tempFile.Close() // 关闭文件以便FFmpeg写入

	// 确保函数结束时清理临时文件
	defer func() {
		if err := os.Remove(tempFilePath); err != nil {
			logrus.Printf("清理临时文件失败: %v", err)
		}
	}()

	// 尝试生成WebP格式缩略图

	// 先尝试提取封面
	if err := extractVideoCover(ctx, videoAbsPath, tempFilePath); err != nil {
		logrus.Printf("提取封面失败，尝试生成3%%处缩略图: %v", err)

		// 尝试生成3%处画面
		if err := extractVideoFrameAtPercentage(ctx, videoAbsPath, tempFilePath, 3.0); err != nil {
			logrus.Printf("生成3%%处缩略图失败: %v", err)
			return
		}
	}

	// 验证WebP文件有效性
	if err := validateWebPFile(tempFilePath); err != nil {
		logrus.Printf("生成的WebP图片无效: %v", err)
		return
	}

	// 确保目标缩略图目录存在
	if err := MakeDir(ctx, targetThumbDir, true); err != nil {
		logrus.Printf("创建目标缩略图目录失败: %v", err)
		return
	}

	// 打开临时文件准备上传
	tempFileReader, err := os.Open(tempFilePath)
	if err != nil {
		logrus.Printf("打开临时文件失败: %v", err)
		return
	}

	defer tempFileReader.Close()

	// 获取临时文件大小
	fileSize := int64(0)
	if info, err := os.Stat(tempFilePath); err == nil {
		fileSize = info.Size()
	}

	// 构造上传流（修改：Mimetype改为image/webp）
	uploadStream := &stream.FileStream{
		Obj: &model.Object{
			Name:     targetThumbName,
			Size:     fileSize,
			Modified: time.Now(),
		},
		Reader:   tempFileReader,
		Mimetype: "image/webp",
	}

	// 上传到目标目录
	if err := fs.PutDirectly(ctx, targetThumbDir, uploadStream, true); err != nil {
		logrus.Printf("上传缩略图到目标路径失败: %v", err)
		return
	}

	logrus.Printf("缩略图生成并上传成功: 临时文件=%s, 目标路径=%s", tempFilePath, targetThumbPath)
}

// 提取视频封面（WebP格式）
func extractVideoCover(ctx context.Context, videoPath, outputPath string) error {
	// 使用libwebp编码器，优化WebP参数
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-i", videoPath,
		"-map", "0:v:0", // 选择第一个视频流
		"-vframes", "1", // 只输出一帧
		"-c:v", "libwebp", // 使用WebP编码器
		"-q:v", "80", // 质量参数（0-100，默认75）
		"-lossless", "0", // 非无损压缩（节省空间）
		"-compression_level", "6", // 压缩级别（0-9，默认6）
		"-preset", "default", // 预设：平衡质量和速度
		"-y", // 覆盖现有文件
		outputPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logrus.Printf("FFmpeg封面提取输出: %s", string(output))
		return err
	}

	return nil
}

// 提取视频指定百分比位置的帧（WebP格式）
func extractVideoFrameAtPercentage(ctx context.Context, videoPath, outputPath string, percentage float64) error {
	// 获取视频时长
	duration, err := getVideoDuration(ctx, videoPath)
	if err != nil {
		return fmt.Errorf("获取视频时长失败: %v", err)
	}

	// 计算目标时间点
	seekTime := duration * (percentage / 100.0)
	seekTimeStr := formatTime(seekTime)

	// 使用libwebp编码器
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-ss", seekTimeStr, // 跳转到指定时间点
		"-i", videoPath,
		"-vframes", "1", // 只输出一帧
		"-vf", "scale=320:-1", // 缩放至320像素宽
		"-c:v", "libwebp", // 使用WebP编码器
		"-q:v", "80", // 质量参数
		"-lossless", "0", // 非无损压缩
		"-compression_level", "6", // 压缩级别
		"-preset", "default", // 预设
		"-update", "1", // 输出单个文件
		"-y", // 覆盖现有文件
		outputPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logrus.Printf("FFmpeg帧提取输出: %s", string(output))
		return err
	}

	return nil
}

// 获取视频时长
func getVideoDuration(ctx context.Context, filePath string) (float64, error) {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		filePath)

	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	duration, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64)
	if err != nil {
		return 0, err
	}

	return duration, nil
}

// 格式化时间为HH:MM:SS.FFF格式
func formatTime(seconds float64) string {
	h := int(seconds / 3600)
	remainingSeconds := seconds - float64(h)*3600
	m := int(remainingSeconds / 60)
	s := remainingSeconds - float64(m)*60
	return fmt.Sprintf("%02d:%02d:%06.3f", h, m, s)
}

// 验证WebP文件有效性
func validateWebPFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 检查文件大小是否大于0
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("获取文件信息失败: %w", err)
	}
	if stat.Size() <= 0 {
		return fmt.Errorf("文件为空")
	}

	// 尝试解码WebP文件
	_, _, err = image.Decode(file)
	if err != nil {
		return fmt.Errorf("WebP解码失败: %w", err)
	}

	return nil
}

// 创建目录（假设已有的函数）
func MakeDir(ctx context.Context, path string, lazyCache ...bool) error {
	err := fs.MakeDir(ctx, path, lazyCache...)
	if err != nil {
		logrus.Errorf("failed make dir %s: %+v", path, err)
	}
	return err
}

func FsForm(c *gin.Context) {
	defer func() {
		if n, _ := io.ReadFull(c.Request.Body, []byte{0}); n == 1 {
			_, _ = utils.CopyWithBuffer(io.Discard, c.Request.Body)
		}
		_ = c.Request.Body.Close()
	}()
	path := c.GetHeader("File-Path")
	path, err := url.PathUnescape(path)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	asTask := c.GetHeader("As-Task") == "true"
	overwrite := c.GetHeader("Overwrite") != "false"
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	path, err = user.JoinPath(path)
	if err != nil {
		common.ErrorResp(c, err, 403)
		return
	}
	if !overwrite {
		if res, _ := fs.Get(c.Request.Context(), path, &fs.GetArgs{NoLog: true}); res != nil {
			common.ErrorStrResp(c, "file exists", 403)
			return
		}
	}
	storage, err := fs.GetStorage(path, &fs.GetStoragesArgs{})
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if storage.Config().NoUpload {
		common.ErrorStrResp(c, "Current storage doesn't support upload", 405)
		return
	}
	file, err := c.FormFile("file")
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	f, err := file.Open()
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	defer f.Close()
	dir, name := stdpath.Split(path)
	h := make(map[*utils.HashType]string)
	if md5 := c.GetHeader("X-File-Md5"); md5 != "" {
		h[utils.MD5] = md5
	}
	if sha1 := c.GetHeader("X-File-Sha1"); sha1 != "" {
		h[utils.SHA1] = sha1
	}
	if sha256 := c.GetHeader("X-File-Sha256"); sha256 != "" {
		h[utils.SHA256] = sha256
	}
	mimetype := file.Header.Get("Content-Type")
	if len(mimetype) == 0 {
		mimetype = utils.GetMimeType(name)
	}
	s := &stream.FileStream{
		Obj: &model.Object{
			Name:     name,
			Size:     file.Size,
			Modified: getLastModified(c),
			HashInfo: utils.NewHashInfoByMap(h),
		},
		Reader:       f,
		Mimetype:     mimetype,
		WebPutAsTask: asTask,
	}
	var t task.TaskExtensionInfo
	if asTask {
		s.Reader = struct {
			io.Reader
		}{f}
		t, err = fs.PutAsTask(c.Request.Context(), dir, s)
	} else {
		err = fs.PutDirectly(c.Request.Context(), dir, s, true)
	}
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	if t == nil {
		common.SuccessResp(c)
		return
	}
	common.SuccessResp(c, gin.H{
		"task": getTaskInfo(t),
	})
}
