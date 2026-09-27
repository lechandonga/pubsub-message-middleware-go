package pubsub

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Envelope 是线上字节流的封装，携带结构版本。
type Envelope struct {
	Version uint32         `json:"version"`
	Body    map[string]any `json:"body"`
}

// Codec 负责消息的序列化与版本兼容校验。
//
// 兼容规则：
//   - 版本必须落在 [MinVersion, MaxVersion]，否则拒绝（ErrVersionMismatch）；
//   - 解码出的字段必须全部在已知字段集合内，否则拒绝（ErrUnknownField）；
//   - 必填字段必须存在且非 null，否则拒绝（ErrMissingField）。
//
// 三种拒绝原因可通过 errors.Is 区分，不会静默跳过。
type Codec struct {
	MinVersion uint32 // 可接受的最低版本
	MaxVersion uint32 // 可接受的最高版本
}

// NewCodec 构造一个限定版本范围的编解码器。
func NewCodec(minVersion, maxVersion uint32) *Codec {
	return &Codec{MinVersion: minVersion, MaxVersion: maxVersion}
}

// Encode 把版本与消息体编码为字节流。版本越界时拒绝编码。
func (c *Codec) Encode(version uint32, body map[string]any) ([]byte, error) {
	if err := c.checkVersion(version); err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{Version: version, Body: body})
}

// Decode 解码并按规则校验：版本范围、未知字段、必填字段。
// required 列出必填字段名；known 列出全部已认识字段名。
// 返回消息版本与消息体。
func (c *Codec) Decode(data []byte, required, known []string) (uint32, map[string]any, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return 0, nil, fmt.Errorf("pubsub: malformed envelope: %w", err)
	}
	if err := c.checkVersion(env.Version); err != nil {
		return 0, nil, err
	}
	if env.Body == nil {
		env.Body = map[string]any{}
	}
	knownSet := make(map[string]struct{}, len(known))
	for _, k := range known {
		knownSet[k] = struct{}{}
	}
	var unknown []string
	for k := range env.Body {
		if _, ok := knownSet[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return 0, nil, fmt.Errorf("%w: %v", ErrUnknownField, unknown)
	}
	for _, k := range required {
		v, ok := env.Body[k]
		if !ok || v == nil {
			return 0, nil, fmt.Errorf("%w: %q", ErrMissingField, k)
		}
	}
	return env.Version, env.Body, nil
}

func (c *Codec) checkVersion(v uint32) error {
	if v < c.MinVersion || v > c.MaxVersion {
		return fmt.Errorf("%w: got v%d, accept [v%d, v%d]",
			ErrVersionMismatch, v, c.MinVersion, c.MaxVersion)
	}
	return nil
}
