package kafka

import (
	"fmt"
	"io"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type RequestHeader struct {
	// Size of the request
	Size int32
	// ID of the API (e.g. produce, fetch, metadata)
	APIKey int16
	// Version of the API to use
	APIVersion int16
	// User defined ID to correlate requests between server and client
	CorrelationID int32
	// Size of the Client ID
	ClientID *string
}

func (r *RequestHeader) Decode(d *kbin.Reader) error {
	r.Size = d.Int32()
	r.APIKey = d.Int16()
	r.APIVersion = d.Int16()
	r.CorrelationID = d.Int32()
	r.ClientID = d.NullableString()
	return d.Complete()
}

func (r *RequestHeader) String() string {
	return fmt.Sprintf(
		"correlation id: %d, api key: %d, client: %v, size: %d",
		r.CorrelationID,
		r.APIKey,
		r.ClientID,
		r.Size,
	)
}

type Request struct {
	Header  RequestHeader
	Request kmsg.Request
}

func (r *Request) Decode(buf []byte) error {
	reader := kbin.Reader{Src: buf}

	err := r.Header.Decode(&reader)
	if err != nil {
		return err
	}

	r.Request = kmsg.RequestForKey(r.Header.APIKey)
	if r.Request == nil {
		return kerr.InvalidRequest
	}
	r.Request.SetVersion(r.Header.APIVersion)

	if r.Request.IsFlexible() {
		// skip the tags in the header
		numTags := reader.Int8()
		for range numTags {
			reader.Bytes() // ignore tags in header
		}
	}

	return r.Request.ReadFrom(reader.Src)
}

type Response struct {
	CorrelationID int32
	Body          kmsg.Response
}

func (r Response) Encode(out io.Writer) (int, error) {
	buf := make([]byte, 8)                         // reserve space for size and correlation id
	if r.Body.IsFlexible() && r.Body.Key() != 18 { // response header not flexible if ApiVersions; see franz-go promisedResp doc
		buf = append(buf, 0)
	}
	buf = r.Body.AppendTo(buf)
	// fmt.Printf("\nRESP:\n%s\n", hex.Dump(buf[8:]))

	kbin.AppendInt32(buf[:0], int32(len(buf))-4)
	kbin.AppendInt32(buf[:4], r.CorrelationID)
	return out.Write(buf)
}
