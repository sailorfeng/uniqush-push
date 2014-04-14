/**
* @file xyqma.go
* @brief Just to support my android push service.
* @author f.f.
* @version 1.0
* @date 2013-10-31
*/

package srv

import (
	"code.google.com/p/goconf/conf"
	"errors"
	"fmt"
	. "github.com/sailorfeng/uniqush-push/push"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type xyqmaPushService struct {
	hostAddr string
}

func newxyqmaPushService() *xyqmaPushService {
	ret := new(xyqmaPushService)
	ret.hostAddr = "127.0.0.1:8999"
	return ret
}

func InstallXyqma() {
	psm := GetPushServiceManager()
	psm.RegisterPushServiceType(newxyqmaPushService())
}

func (p *xyqmaPushService) Finalize() {}

func (p *xyqmaPushService) Config(cf *conf.ConfigFile) {
	v, err := cf.GetString(p.Name(), "serv")
	if err == nil {
		p.hostAddr = v
	}
}

func (p *xyqmaPushService) BuildPushServiceProviderFromMap(kv map[string]string,
	psp *PushServiceProvider) error {
	if service, ok := kv["service"]; ok && len(service) > 0 {
		psp.FixedData["service"] = service
	} else {
		return errors.New("NoService")
	}

	return nil
}

func (p *xyqmaPushService) BuildDeliveryPointFromMap(kv map[string]string,
	dp *DeliveryPoint) error {
	if service, ok := kv["service"]; ok && len(service) > 0 {
		dp.FixedData["service"] = service
	} else {
		return errors.New("NoService")
	}
	if sub, ok := kv["subscriber"]; ok && len(sub) > 0 {
		dp.FixedData["subscriber"] = sub
	} else {
		return errors.New("NoSubscriber")
	}

	if regid, ok := kv["regid"]; ok && len(regid) > 0 {
		dp.FixedData["regid"] = regid
	} else {
		return errors.New("NoRegId")
	}

	return nil
}

func (p *xyqmaPushService) Name() string {
	return "xyqma"
}

func (p *xyqmaPushService) singlePush(psp *PushServiceProvider, dp *DeliveryPoint, n *Notification) (string, error) {
	if psp.PushServiceName() != dp.PushServiceName() || psp.PushServiceName() != p.Name() {
		return "", NewIncompatibleError()
	}

	msg := n.Data
	data := url.Values{}
	regid := dp.FixedData["regid"]
	if len(regid) == 0 {
		reterr := NewBadDeliveryPointWithDetails(dp, "EmptyRegistrationID")
		return "", reterr
	}
	data.Set("dev_id", regid)

	for k, v := range msg {
		switch k {
		case "id":
			continue
		default:
			data.Set(k, v)
		}
	}

	req, err := http.NewRequest("GET", "http://"+ p.hostAddr + "/push?" + data.Encode(), strings.NewReader(""))
	if err != nil {
		return "", err
	}

	client := &http.Client{}

	r, e20 := client.Do(req)
	if e20 != nil {
		return "", e20
	}
	defer r.Body.Close()

	switch r.StatusCode {
	case 503:
		fallthrough
	case 500:
		after := 0 * time.Second
		var reterr error
		reterr = NewRetryError(psp, dp, n, after)
		return "", reterr
	case 401:
		return "", NewBadPushServiceProvider(psp)
	case 400:
		return "", NewBadNotification()
	}

	contents, e30 := ioutil.ReadAll(r.Body)
	if e30 != nil {
		return "", e30
	}

	retid := fmt.Sprintf("xyqma:%s-%s", psp.Name(), string(contents))
	return retid, nil
}

func (self *xyqmaPushService) Push(psp *PushServiceProvider, dpQueue <-chan *DeliveryPoint, resQueue chan<- *PushResult, notif *Notification) {
	wg := new(sync.WaitGroup)
	for dp := range dpQueue {
		wg.Add(1)
		go func() {
			res := new(PushResult)
			res.Provider = psp
			res.Destination = dp
			res.Content = notif
			msgid, err := self.singlePush(psp, dp, notif)
			if err != nil {
				res.Err = err
			} else {
				res.MsgId = msgid
			}
			resQueue <- res
			wg.Done()
		}()
	}

	wg.Wait()
	close(resQueue)
}

func (self *xyqmaPushService) SetErrorReportChan(errChan chan<- error) {
	return
}
