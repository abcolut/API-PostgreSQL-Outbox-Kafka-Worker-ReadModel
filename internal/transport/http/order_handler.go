package http

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"my-go-service/internal/domain"
	"my-go-service/internal/repository/postgres"
	"my-go-service/internal/service"
)

type OrderHandler struct {
	svc *service.OrderService
}

func NewOrderHandler(svc *service.OrderService) *OrderHandler {
	return &OrderHandler{svc: svc}
}

// ---------- POST /orders ----------

func (h *OrderHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.UserID <= 0 || strings.TrimSpace(req.Item) == "" || req.Quantity <= 0 {
		writeError(w, http.StatusBadRequest, "user_id, item, quantity are required")
		return
	}

	order, err := h.svc.Create(r.Context(), req)
	if err != nil {
		log.Printf("create order: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, order)
}

// ---------- GET /orders/{id} ----------

func (h *OrderHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id, err := parseIDFromPath(r.URL.Path, "/orders/")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	order, err := h.svc.GetByID(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		log.Printf("get order: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, order)
}

// ---------- GET /orders?user_id=1&status=created&item=book&limit=20&offset=0 ----------

func (h *OrderHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := domain.OrderFilter{}

	if v := q.Get("user_id"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil || id <= 0 {
			writeError(w, http.StatusBadRequest, "invalid user_id")
			return
		}
		filter.UserID = &id
	}
	if v := q.Get("status"); v != "" {
		filter.Status = &v
	}
	if v := q.Get("item"); v != "" {
		filter.Item = &v
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		filter.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid offset")
			return
		}
		filter.Offset = n
	}

	orders, err := h.svc.List(r.Context(), filter)
	if err != nil {
		log.Printf("list orders: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if orders == nil {
		orders = []domain.Order{} // чтобы JSON был [], а не null
	}
	writeJSON(w, http.StatusOK, orders)
}

// ---------- PATCH /orders/{id} ----------

func (h *OrderHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := parseIDFromPath(r.URL.Path, "/orders/")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	var req domain.UpdateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.Item != nil && strings.TrimSpace(*req.Item) == "" {
		writeError(w, http.StatusBadRequest, "item must not be empty")
		return
	}
	if req.Quantity != nil && *req.Quantity <= 0 {
		writeError(w, http.StatusBadRequest, "quantity must be positive")
		return
	}
	if req.Status != nil && strings.TrimSpace(*req.Status) == "" {
		writeError(w, http.StatusBadRequest, "status must not be empty")
		return
	}

	order, err := h.svc.Update(r.Context(), id, req)
	if errors.Is(err, postgres.ErrNoFieldsToUpdate) {
		writeError(w, http.StatusBadRequest, "no fields to update")
		return
	}
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		log.Printf("update order: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, order)
}

// ---------- DELETE /orders/{id} ----------

func (h *OrderHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := parseIDFromPath(r.URL.Path, "/orders/")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	err = h.svc.Delete(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		log.Printf("delete order: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent) // 204
}

// ---------- helpers ----------

func parseIDFromPath(path, prefix string) (int64, error) {
	raw := strings.TrimPrefix(path, prefix)
	raw = strings.TrimSuffix(raw, "/")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("invalid id")
	}
	return id, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
