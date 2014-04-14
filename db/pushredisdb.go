/*
 * Copyright 2011 Nan Deng
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
 *
 */

package db

import (
	"errors"
	"fmt"
	"time"
	redis "github.com/monnand/goredis"
	. "github.com/sailorfeng/uniqush-push/push"
	"strconv"
	"strings"
	"math"
	"regexp"
)

type PushRedisDB struct {
	client *redis.Client
	psm    *PushServiceManager
	afkUserTime int
}

const (
	DELIVERY_POINT_PREFIX                                 string = "delivery.point:"
	PUSH_SERVICE_PROVIDER_PREFIX                          string = "push.service.provider:"
	SERVICE_SUBSCRIBER_TO_DELIVERY_POINTS_PREFIX          string = "srv.sub-2-dp:"
	SERVICE_DELIVERY_POINT_TO_PUSH_SERVICE_RPVIDER_PREFIX string = "srv.dp-2-psp:"
	SERVICE_TO_PUSH_SERVICE_PROVIDERS_PREFIX              string = "srv-2-psp:"
	DELIVERY_POINT_COUNTER_PREFIX                         string = "delivery.point.counter:"
)

func newPushRedisDB(c *DatabaseConfig) (*PushRedisDB, error) {
	if c == nil {
		return nil, errors.New("Invalid Database Config")
	}
	if strings.ToLower(c.Engine) != "redis" {
		return nil, errors.New("Unsupported Database Engine")
	}
	var client redis.Client
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port <= 0 {
		c.Port = 6379
	}
	if c.Name == "" {
		c.Name = "0"
	}

	client.Addr = fmt.Sprintf("%s:%d", c.Host, c.Port)
	client.Password = c.Password
	var err error
	client.Db, err = strconv.Atoi(c.Name)
	if err != nil {
		client.Db = 0
	}

	ret := new(PushRedisDB)
	ret.client = &client
	ret.psm = c.PushServiceManager
	ret.afkUserTime = c.AfkUserTime
	if ret.psm == nil {
		ret.psm = GetPushServiceManager()
	}
	return ret, nil
}

func (r *PushRedisDB) keyValueToDeliveryPoint(name string, value []byte) *DeliveryPoint {
	psm := r.psm
	dp, err := psm.BuildDeliveryPointFromBytes(value)
	if err != nil {
		return nil
	}
	return dp
}

func (r *PushRedisDB) keyValueToPushServiceProvider(name string, value []byte) *PushServiceProvider {
	psm := r.psm
	psp, err := psm.BuildPushServiceProviderFromBytes(value)
	if err != nil {
		return nil
	}
	return psp
}

func deliveryPointToValue(dp *DeliveryPoint) []byte {
	return dp.Marshal()
}

func pushServiceProviderToValue(psp *PushServiceProvider) []byte {
	return psp.Marshal()
}

func (r *PushRedisDB) GetDeliveryPoint(name string) (*DeliveryPoint, error) {
	b, err := r.client.Get(DELIVERY_POINT_PREFIX + name)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	return r.keyValueToDeliveryPoint(name, b), nil
}

func (r *PushRedisDB) SetDeliveryPoint(dp *DeliveryPoint) error {
	err := r.client.Set(DELIVERY_POINT_PREFIX+dp.Name(), deliveryPointToValue(dp))
	return err
}

func (r *PushRedisDB) GetPushServiceProvider(name string) (*PushServiceProvider, error) {
	b, err := r.client.Get(PUSH_SERVICE_PROVIDER_PREFIX + name)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	return r.keyValueToPushServiceProvider(name, b), nil
}

func (r *PushRedisDB) SetPushServiceProvider(psp *PushServiceProvider) error {
	return r.client.Set(PUSH_SERVICE_PROVIDER_PREFIX+psp.Name(), pushServiceProviderToValue(psp))
}

func (r *PushRedisDB) RemoveDeliveryPoint(dp string) error {
	_, err := r.client.Del(DELIVERY_POINT_PREFIX + dp)
	return err
}

func (r *PushRedisDB) RemovePushServiceProvider(psp string) error {
	_, err := r.client.Del(PUSH_SERVICE_PROVIDER_PREFIX + psp)
	return err
}

const (
	OP_EQUAL = 0x1
	OP_LT = 0x02
	OP_GT = 0x04
)

type filterStruct struct {
	op int
	val string
	numVal int
}

func genFilter(filter string) (map[string]*filterStruct, error) {
	if len(filter) == 0 {
		return nil, nil
	}
	filterOpRe := regexp.MustCompile("[=<>]+")
	splitFilter := strings.Split(filter, "&&")
	filterMap := make(map[string]*filterStruct)
	for _, filterStr := range splitFilter {
		opStr := filterOpRe.FindString(filterStr)
		spF := filterOpRe.Split(filterStr, 2)

		bm := spF[0]
		val := spF[1]
		opVal := 0
		for _, cV := range opStr {
			switch(cV) {
			case '=':
				opVal |= OP_EQUAL
			case '>':
				opVal |= OP_GT
			case '<':
				opVal |= OP_LT
			default:
				return nil, fmt.Errorf("filter format error:%s", filter)
			}
		}

		filterMap[bm] = new(filterStruct)
		filterMap[bm].op = opVal
		filterMap[bm].val = val
		filterMap[bm].numVal = math.MinInt32
		if intVal, canConv := strconv.Atoi(val); canConv == nil {
			filterMap[bm].numVal = intVal
		}
	}
	return filterMap, nil
}

func checkByFilter(sRet map[string]string, filter map[string]*filterStruct) (bool) {
	for bm, fv := range filter {
		rV, hasVal := sRet[bm]
		if !hasVal {
			if fv.op == OP_EQUAL && fv.val == "" {
				continue
			}
			return false
		} else {
			if fv.numVal != math.MinInt32 {
				if intVal, canConv := strconv.Atoi(rV); canConv != nil {
					return false
				} else {
					switch(fv.op) {
					case OP_EQUAL:
						if intVal != fv.numVal {
							return false
						}
					case OP_GT:
						if intVal <= fv.numVal {
							return false
						}
					case OP_GT | OP_EQUAL:
						if intVal < fv.numVal {
							return false
						}
					case OP_LT:
						if intVal >= fv.numVal {
							return false
						}
					case OP_LT | OP_EQUAL:
						if intVal > fv.numVal {
							return false
						}
					case OP_LT | OP_GT:
						if intVal == fv.numVal {
							return false
						}
					}
				}
			} else {
				switch(fv.op) {
				case OP_EQUAL:
					if rV != fv.val {
						return false
					}
				case OP_GT:
					if rV <= fv.val {
						return false
					}
				case OP_GT | OP_EQUAL:
					if rV < fv.val {
						return false
					}
				case OP_LT:
					if rV >= fv.val {
						return false
					}
				case OP_LT | OP_EQUAL:
					if rV > fv.val {
						return false
					}
				case OP_LT | OP_GT:
					if rV == fv.val {
						return false
					}
				}
			}
		}
	}
	return true
}

// set attrib for subscriber add by f.f. 2013.10.10
func (r *PushRedisDB) SetAttribToServiceSubscriber(srv, sub string, attribs map[string]string) error {
	var err error
	subStr := SERVICE_SUBSCRIBER_TO_DELIVERY_POINTS_PREFIX+srv+":"+sub
	if hasKey, err := r.client.Hlen(subStr); err != nil || hasKey == 0 {
		if err != nil {
			return err
		} else {
			return fmt.Errorf("NoSuchSubscriber:%v", sub)
		}
	}

	modVal := make(map[string]string)
	for attr, val := range attribs {
		if len(val) == 0 {
			_, err = r.client.Hdel(subStr, attr)
			if err != nil {
				return err
			}

		} else {
			modVal[attr] = val
		}
	}

	err = r.client.Hmset(subStr, modVal)
	if err != nil {
		return err
	}

	return nil
}

func (r *PushRedisDB) GetDeliveryPointsNameByServiceSubscriber(srv, usr, filter string) (map[string][]string, error) {
	keys := make([]string, 1)
	if !strings.Contains(usr, "*") && !strings.Contains(srv, "*") {
		keys[0] = SERVICE_SUBSCRIBER_TO_DELIVERY_POINTS_PREFIX + srv + ":" + usr
	} else {
		var err error
		keys, err = r.client.Keys(SERVICE_SUBSCRIBER_TO_DELIVERY_POINTS_PREFIX + srv + ":" + usr)
		if err != nil {
			return nil, err
		}
	}

	nowUnixSec := time.Now().Unix()
	ret := make(map[string][]string, len(keys))

	filterMap, err := genFilter(filter)
	if err != nil {
		return nil, err
	}

	justPushLast := false
	if filterMap != nil {
		_, justPushLast = filterMap["LastestDevice"]
	}

	for _, k := range keys {
		sRet := make(map[string]string)
		err := r.client.Hgetall(k, sRet)
		if err != nil {
			return nil, err
		}

		if filterMap != nil && !checkByFilter(sRet, filterMap) {
			continue
		}

		elem := strings.Split(k, ":")
		s := elem[1]
		if l, ok := ret[s]; !ok || l == nil {
			ret[s] = make([]string, 0, len(keys))
		}

		var lastestTime int64
		lastestTime = 0
		var lastestDp string

		subDp := make([]string, 0)
		for bm, v := range sRet {
			if strings.Index(bm, ":") < 0 {
				continue
			}
			lastActTime, canConv := strconv.ParseInt(v, 10, 64)
			if canConv != nil {
				lastActTime = 0
			}
			afkTime := nowUnixSec - lastActTime
			if afkTime > 3600*24*365 {
				// TODO: del this user
				continue
			} else if r.afkUserTime > 0 && afkTime > (int64)(r.afkUserTime) {
				// away to long ago
				continue
			}

			if lastActTime > lastestTime {
				lastestTime = lastActTime
				lastestDp = string(bm)
				//fmt.Printf("user:%v lastActTime:%v lastestTime:%v, lastestDp:%v\n", k, lastActTime, lastestTime, lastestDp)
			}

			subDp = append(subDp, string(bm))
		}

		//fmt.Printf("user:%v justPushLast:%v len:%v lastestTime:%v, lastestDp:%v\n", k, justPushLast, len(ret[s]), lastestTime, lastestDp)
		if justPushLast && lastestTime > 0 {
			ret[s] = append(ret[s], lastestDp)
			//fmt.Printf("push to just last device:%v astestDp:%v\n", s, lastestDp)
		} else {
			ret[s] = append(ret[s], subDp...)
		}
		fmt.Printf("push to device:%v dps:%#v\n", s, ret[s])
	}
	return ret, nil
}

func (r *PushRedisDB) GetPushServiceProviderNameByServiceDeliveryPoint(srv, dp string) (string, error) {
	b, err := r.client.Get(SERVICE_DELIVERY_POINT_TO_PUSH_SERVICE_RPVIDER_PREFIX + srv + ":" + dp)
	if err != nil {
		return "", err
	}
	if b == nil {
		return "", nil
	}
	return string(b), nil
}

func (r *PushRedisDB) AddDeliveryPointToServiceSubscriber(srv, sub, dp string) error {
	timeStr := fmt.Sprintf("%d", time.Now().Unix())
	subStr := SERVICE_SUBSCRIBER_TO_DELIVERY_POINTS_PREFIX+srv+":"+sub

	i, err := r.client.Hset(subStr, dp, []byte(timeStr))
	if err != nil {
		return err
	}
	if i == false {
		return nil
	}

	_, err = r.client.Incr(DELIVERY_POINT_COUNTER_PREFIX + dp)
	if err != nil {
		return err
	}
	return nil
}

func (r *PushRedisDB) RemoveDeliveryPointFromServiceSubscriber(srv, sub, dp string) error {
	j, err := r.client.Hdel(SERVICE_SUBSCRIBER_TO_DELIVERY_POINTS_PREFIX+srv+":"+sub, dp)
	if err != nil {
		return err
	}
	if j == false {
		return nil
	}
	i, e := r.client.Decr(DELIVERY_POINT_COUNTER_PREFIX + dp)
	if e != nil {
		return e
	}
	if i <= 0 {
		_, e0 := r.client.Del(DELIVERY_POINT_COUNTER_PREFIX + dp)
		if e0 != nil {
			return e0
		}
		_, e1 := r.client.Del(DELIVERY_POINT_PREFIX + dp)
		return e1
	}
	return e
}

func (r *PushRedisDB) SetPushServiceProviderOfServiceDeliveryPoint(srv, dp, psp string) error {
	return r.client.Set(SERVICE_DELIVERY_POINT_TO_PUSH_SERVICE_RPVIDER_PREFIX+srv+":"+dp, []byte(psp))
}

func (r *PushRedisDB) RemovePushServiceProviderOfServiceDeliveryPoint(srv, dp string) error {
	_, err := r.client.Del(SERVICE_DELIVERY_POINT_TO_PUSH_SERVICE_RPVIDER_PREFIX + srv + ":" + dp)
	return err
}

func (r *PushRedisDB) GetPushServiceProvidersByService(srv string) ([]string, error) {
	m, err := r.client.Smembers(SERVICE_TO_PUSH_SERVICE_PROVIDERS_PREFIX + srv)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, nil
	}
	ret := make([]string, len(m))
	for i, bm := range m {
		ret[i] = string(bm)
	}

	return ret, nil
}

func (r *PushRedisDB) RemovePushServiceProviderFromService(srv, psp string) error {
	_, err := r.client.Srem(SERVICE_TO_PUSH_SERVICE_PROVIDERS_PREFIX+srv, []byte(psp))
	return err
}

func (r *PushRedisDB) AddPushServiceProviderToService(srv, psp string) error {
	_, err := r.client.Sadd(SERVICE_TO_PUSH_SERVICE_PROVIDERS_PREFIX+srv, []byte(psp))
	return err
}

func (r *PushRedisDB) FlushCache() error {
	return r.client.Save()
}
