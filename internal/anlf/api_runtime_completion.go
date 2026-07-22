package anlf

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/processor"
)

func (s *Server) HandleRuntimeCompletion(c *gin.Context) {
	subscriptionID := c.Param("subscriptionId")
	var event contract.RuntimeCompletionEvent
	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Malformed runtime completion event"})
		return
	}
	if subscriptionID != event.SubscriptionID {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Path subscription ID does not match request body"})
		return
	}
	if err := event.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	if err := s.processor.HandleRuntimeCompletion(&event); err != nil {
		if errors.Is(err, processor.ErrFutureRuntimeRevision) {
			c.JSON(http.StatusConflict, gin.H{"detail": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Runtime completion handling failed"})
		return
	}
	c.Status(http.StatusNoContent)
}
