/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2023 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
)

// expirationDateFormat date format for expiration key in json policy.
const expirationDateFormat = "2006-01-02T15:04:05.000Z"

// policyCondition explanation:
// http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
//
// Example:
//
//	policyCondition {
//	    matchType: "$eq",
//	    key: "$Content-Type",
//	    value: "image/png",
//	}
type policyCondition struct {
	matchType string
	condition string
	value     string
}

// PostPolicy - Provides strict static type conversion and validation
// for Amazon S3's POST policy JSON string.
type PostPolicy struct {
	// Expiration date and time of the POST policy.
	expiration time.Time
	// Collection of different policy conditions.
	conditions []policyCondition
	// ContentLengthRange minimum and maximum allowable size for the
	// uploaded content.
	contentLengthRange struct {
		min int64
		max int64
	}

	// Post form data.
	formData map[string]string
}

// NewPostPolicy - Instantiate new post policy.
func NewPostPolicy() *PostPolicy {
	p := &PostPolicy{}
	p.conditions = make([]policyCondition, 0)
	p.formData = make(map[string]string)
	return p
}

// SetExpires - Sets expiration time for the new policy.
func (p *PostPolicy) SetExpires(t time.Time) error {
	if t.IsZero() {
		return errInvalidArgument("No expiry time set.")
	}
	p.expiration = t
	return nil
}

// SetKey - Sets an object name for the policy based upload.
func (p *PostPolicy) SetKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return errInvalidArgument("Object name is empty.")
	}
	policyCond := policyCondition{
		matchType: "eq",
		condition: "$key",
		value:     key,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["key"] = key
	return nil
}

// SetKeyStartsWith - Sets an object name that an policy based upload
// can start with.
// Can use an empty value ("") to allow any key.
func (p *PostPolicy) SetKeyStartsWith(keyStartsWith string) error {
	policyCond := policyCondition{
		matchType: "starts-with",
		condition: "$key",
		value:     keyStartsWith,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["key"] = keyStartsWith
	return nil
}

// SetBucket - Sets bucket at which objects will be uploaded to.
func (p *PostPolicy) SetBucket(bucketName string) error {
	if strings.TrimSpace(bucketName) == "" {
		return errInvalidArgument("Bucket name is empty.")
	}
	policyCond := policyCondition{
		matchType: "eq",
		condition: "$bucket",
		value:     bucketName,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["bucket"] = bucketName
	return nil
}

// SetCondition - Sets condition for credentials, date and algorithm
func (p *PostPolicy) SetCondition(matchType, condition, value string) error {
	if strings.TrimSpace(value) == "" {
		return errInvalidArgument("No value specified for condition")
	}

	policyCond := policyCondition{
		matchType: matchType,
		condition: "$" + condition,
		value:     value,
	}
	if condition == "X-Amz-Credential" || condition == "X-Amz-Date" || condition == "X-Amz-Algorithm" {
		if err := p.addNewPolicy(policyCond); err != nil {
			return err
		}
		p.formData[condition] = value
		return nil
	}
	return errInvalidArgument("Invalid condition in policy")
}

// SetTagging - Sets tagging for the object for this policy based upload.
func (p *PostPolicy) SetTagging(tagging string) error {
	if strings.TrimSpace(tagging) == "" {
		return errInvalidArgument("No tagging specified.")
	}
	_, err := tags.ParseObjectXML(strings.NewReader(tagging))
	if err != nil {
		return errors.New(s3ErrorResponseMap[MalformedXML]) //nolint
	}
	policyCond := policyCondition{
		matchType: "eq",
		condition: "$tagging",
		value:     tagging,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["tagging"] = tagging
	return nil
}

// SetContentType - Sets content-type of the object for this policy
// based upload.
func (p *PostPolicy) SetContentType(contentType string) error {
	if strings.TrimSpace(contentType) == "" {
		return errInvalidArgument("No content type specified.")
	}
	policyCond := policyCondition{
		matchType: "eq",
		condition: "$Content-Type",
		value:     contentType,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["Content-Type"] = contentType
	return nil
}

// SetContentTypeStartsWith - Sets what content-type of the object for this policy
// based upload can start with.
// Can use an empty value ("") to allow any content-type.
func (p *PostPolicy) SetContentTypeStartsWith(contentTypeStartsWith string) error {
	policyCond := policyCondition{
		matchType: "starts-with",
		condition: "$Content-Type",
		value:     contentTypeStartsWith,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["Content-Type"] = contentTypeStartsWith
	return nil
}

// SetContentDisposition - Sets content-disposition of the object for this policy
func (p *PostPolicy) SetContentDisposition(contentDisposition string) error {
	if strings.TrimSpace(contentDisposition) == "" {
		return errInvalidArgument("No content disposition specified.")
	}
	policyCond := policyCondition{
		matchType: "eq",
		condition: "$Content-Disposition",
		value:     contentDisposition,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["Content-Disposition"] = contentDisposition
	return nil
}

// SetContentEncoding - Sets content-encoding of the object for this policy
func (p *PostPolicy) SetContentEncoding(contentEncoding string) error {
	if strings.TrimSpace(contentEncoding) == "" {
		return errInvalidArgument("No content encoding specified.")
	}
	policyCond := policyCondition{
		matchType: "eq",
		condition: "$Content-Encoding",
		value:     contentEncoding,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["Content-Encoding"] = contentEncoding
	return nil
}

// SetContentLengthRange - Set new min and max content length
// condition for all incoming uploads.
func (p *PostPolicy) SetContentLengthRange(minLen, maxLen int64) error {
	if minLen > maxLen {
		return errInvalidArgument("Minimum limit is larger than maximum limit.")
	}
	if minLen < 0 {
		return errInvalidArgument("Minimum limit cannot be negative.")
	}
	if maxLen <= 0 {
		return errInvalidArgument("Maximum limit cannot be non-positive.")
	}
	p.contentLengthRange.min = minLen
	p.contentLengthRange.max = maxLen
	return nil
}

// SetSuccessActionRedirect - Sets the redirect success url of the object for this policy
// based upload.
func (p *PostPolicy) SetSuccessActionRedirect(redirect string) error {
	if strings.TrimSpace(redirect) == "" {
		return errInvalidArgument("Redirect is empty")
	}
	policyCond := policyCondition{
		matchType: "eq",
		condition: "$success_action_redirect",
		value:     redirect,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["success_action_redirect"] = redirect
	return nil
}

// SetSuccessStatusAction - Sets the status success code of the object for this policy
// based upload.
func (p *PostPolicy) SetSuccessStatusAction(status string) error {
	if strings.TrimSpace(status) == "" {
		return errInvalidArgument("Status is empty")
	}
	policyCond := policyCondition{
		matchType: "eq",
		condition: "$success_action_status",
		value:     status,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData["success_action_status"] = status
	return nil
}

// SetUserMetadata - Set user metadata as a key/value couple.
// Can be retrieved through a HEAD request or an event.
func (p *PostPolicy) SetUserMetadata(key, value string) error {
	if strings.TrimSpace(key) == "" {
		return errInvalidArgument("Key is empty")
	}
	if strings.TrimSpace(value) == "" {
		return errInvalidArgument("Value is empty")
	}
	headerName := fmt.Sprintf("x-amz-meta-%s", key)
	policyCond := policyCondition{
		matchType: "eq",
		condition: fmt.Sprintf("$%s", headerName),
		value:     value,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData[headerName] = value
	return nil
}

// SetUserMetadataStartsWith - Set how an user metadata should starts with.
// Can be retrieved through a HEAD request or an event.
func (p *PostPolicy) SetUserMetadataStartsWith(key, value string) error {
	if strings.TrimSpace(key) == "" {
		return errInvalidArgument("Key is empty")
	}
	headerName := fmt.Sprintf("x-amz-meta-%s", key)
	policyCond := policyCondition{
		matchType: "starts-with",
		condition: fmt.Sprintf("$%s", headerName),
		value:     value,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData[headerName] = value
	return nil
}

// SetChecksum sets the checksum of the request.
func (p *PostPolicy) SetChecksum(c Checksum) error {
	if c.IsSet() {
		p.formData[amzChecksumAlgo] = c.Type.String()
		p.formData[c.Type.Key()] = c.Encoded()

		policyCond := policyCondition{
			matchType: "eq",
			condition: fmt.Sprintf("$%s", amzChecksumAlgo),
			value:     c.Type.String(),
		}
		if err := p.addNewPolicy(policyCond); err != nil {
			return err
		}
		policyCond = policyCondition{
			matchType: "eq",
			condition: fmt.Sprintf("$%s", c.Type.Key()),
			value:     c.Encoded(),
		}
		if err := p.addNewPolicy(policyCond); err != nil {
			return err
		}
	}
	return nil
}

// SetEncryption - sets encryption headers for POST API
func (p *PostPolicy) SetEncryption(sse encrypt.ServerSide) {
	if sse == nil {
		return
	}
	h := http.Header{}
	sse.Marshal(h)
	for k, v := range h {
		p.formData[k] = v[0]
	}
}

// SetUserData - Set user data as a key/value couple.
// Can be retrieved through a HEAD request or an event.
func (p *PostPolicy) SetUserData(key, value string) error {
	if key == "" {
		return errInvalidArgument("Key is empty")
	}
	if value == "" {
		return errInvalidArgument("Value is empty")
	}
	headerName := fmt.Sprintf("x-amz-%s", key)
	policyCond := policyCondition{
		matchType: "eq",
		condition: fmt.Sprintf("$%s", headerName),
		value:     value,
	}
	if err := p.addNewPolicy(policyCond); err != nil {
		return err
	}
	p.formData[headerName] = value
	return nil
}

// addNewPolicy - internal helper to validate adding new policies.
// Can use starts-with with an empty value ("") to allow any content within a form field.
func (p *PostPolicy) addNewPolicy(policyCond policyCondition) error {
	if policyCond.matchType == "" || policyCond.condition == "" {
		return errInvalidArgument("Policy fields are empty.")
	}
	if policyCond.matchType != "starts-with" && policyCond.value == "" {
		return errInvalidArgument("Policy value is empty.")
	}
	p.conditions = append(p.conditions, policyCond)
	return nil
}

// String function for printing policy in json formatted string.
func (p PostPolicy) String() string {
	return string(p.marshalJSON())
}

// marshalJSON - Provides Marshaled JSON in bytes.
func (p PostPolicy) marshalJSON() []byte {
	expirationStr := `"expiration":"` + p.expiration.UTC().Format(expirationDateFormat) + `"`
	var conditionsStr string
	conditions := []string{}
	for _, po := range p.conditions {
		conditions = append(conditions, fmt.Sprintf("[\"%s\",\"%s\",\"%s\"]", po.matchType, po.condition, po.value))
	}
	if p.contentLengthRange.min != 0 || p.contentLengthRange.max != 0 {
		conditions = append(conditions, fmt.Sprintf("[\"content-length-range\", %d, %d]",
			p.contentLengthRange.min, p.contentLengthRange.max))
	}
	if len(conditions) > 0 {
		conditionsStr = `"conditions":[` + strings.Join(conditions, ",") + "]"
	}
	retStr := "{"
	retStr = retStr + expirationStr + ","
	retStr += conditionsStr
	retStr += "}"
	return []byte(retStr)
}

// base64 - Produces base64 of PostPolicy's Marshaled json.
func (p PostPolicy) base64() string {
	return base64.StdEncoding.EncodeToString(p.marshalJSON())
}
