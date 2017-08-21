package workqueue

import (
	"time"

	"github.com/golang/glog"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
)

// ErrorHandler should do something something sensible when the consumer fails.
// ErrorHandler is also responsible for doing something sensible if there is no error
type ErrorHandler func(error, interface{})

// Consumer consumes a work item. If consumer returns an error ErrorHandler will be invoked
type Consumer func(interface{}) error

// ParralelizedRateLimitedQueueConsumer TODO commment
type ParralelizedRateLimitedQueueConsumer struct {
	Queue           RateLimitingInterface
	ConsumeFunc     Consumer
	HandleErrorFunc ErrorHandler
}

// HandleErrorOrGiveUpAfter provides a default behavior to handling errors. If numrequeues > maxretries the key is dropped
func HandleErrorOrGiveUpAfter(maxRetries int, errorLabel string, queue RateLimitingInterface) ErrorHandler {
	return func(err error, key interface{}) {
		if err == nil {
			queue.Forget(key)
			return
		}

		if queue.NumRequeues(key) < maxRetries {
			glog.V(2).Infof("Error syncing %v %v: %v", errorLabel, key, err)
			queue.AddRateLimited(key)
			return
		}

		utilruntime.HandleError(err)
		glog.V(2).Infof("Dropping %v %q out of the queue: %v", errorLabel, key, err)
		queue.Forget(key)
	}
}

// Run runs it
func (p *ParralelizedRateLimitedQueueConsumer) Run(workers int, period time.Duration, stopCh <-chan struct{}) {
	for i := 0; i < workers; i++ {
		go wait.Until(p.worker, period, stopCh)
	}

	<-stopCh
}

func (p *ParralelizedRateLimitedQueueConsumer) worker() {
	for p.processNextWorkItem() {
	}
}

func (p *ParralelizedRateLimitedQueueConsumer) processNextWorkItem() bool {
	key, quit := p.Queue.Get()
	if quit {
		return false
	}
	defer p.Queue.Done(key)

	err := p.ConsumeFunc(key)
	p.HandleErrorFunc(err, key)

	return true
}
