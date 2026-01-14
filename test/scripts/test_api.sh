#!/bin/bash
# NWDAF API Test Script
# Usage: ./test_api.sh [create|delete|all]

BASE_URL="http://127.0.0.1:8080/nnwdaf-eventssubscription/v1"
SUBSCRIPTION_ID=""

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }
log_info() { echo -e "${YELLOW}ℹ️  $1${NC}"; }

# Test 1: Create valid UE_COMMUNICATION subscription with supis
test_create_valid() {
    log_info "Test: Create valid UE_COMMUNICATION subscription (with supis)"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "UE_COMMUNICATION",
                "tgtUe": {"supis": ["imsi-208930000000003"]}
            }],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "201" ]; then
        log_success "Created successfully (201)"
        SUBSCRIPTION_ID=$(echo "$BODY" | jq -r '.subscriptionId // empty')
        echo "Response: $BODY" | jq .
    else
        log_error "Creation failed (HTTP $HTTP_CODE)"
        echo "$BODY" | jq .
    fi
}

# Test 2: UE_COMMUNICATION with intGroupIds
test_ue_comm_intgroupids() {
    log_info "Test: UE_COMMUNICATION with intGroupIds"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "UE_COMMUNICATION",
                "tgtUe": {"intGroupIds": ["group-001"]}
            }],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "201" ]; then
        log_success "Created successfully (201)"
        echo "$BODY" | jq .
    else
        log_error "Creation failed (HTTP $HTTP_CODE)"
        echo "$BODY" | jq .
    fi
}

# Test 3: UE_COMMUNICATION missing tgtUe (should reject)
test_ue_comm_missing_tgtue() {
    log_info "Test: UE_COMMUNICATION missing tgtUe (should reject)"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "UE_COMMUNICATION"
            }],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "400" ]; then
        CAUSE=$(echo "$BODY" | jq -r '.cause')
        if [ "$CAUSE" = "INVALID_REQUEST" ]; then
            log_success "Correctly rejected (400) - INVALID_REQUEST"
        else
            log_error "Wrong cause: $CAUSE (expected INVALID_REQUEST)"
        fi
    else
        log_error "Expected 400, got HTTP $HTTP_CODE"
    fi
    echo "$BODY" | jq .
}

# Test 4: UE_COMMUNICATION missing supis/intGroupIds (should reject)
test_ue_comm_missing_identifiers() {
    log_info "Test: UE_COMMUNICATION with empty tgtUe (should reject)"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "UE_COMMUNICATION",
                "tgtUe": {}
            }],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "400" ]; then
        log_success "Correctly rejected (400)"
    else
        log_error "Expected 400, got HTTP $HTTP_CODE"
    fi
    echo "$BODY" | jq .
}

# Test 5: Unsupported event type (UE_MOBILITY)
test_unsupported_event() {
    log_info "Test: Unsupported event type (UE_MOBILITY)"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "UE_MOBILITY",
                "tgtUe": {"supis": ["imsi-123456789"]}
            }],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "400" ]; then
        log_success "Correctly rejected (400)"
    else
        log_error "Should reject but returned HTTP $HTTP_CODE"
    fi
    echo "$BODY" | jq .
}

# Test 6: ABNORMAL_BEHAVIOUR now returns failEventReports
test_abnormal_now_unsupported() {
    log_info "Test: ABNORMAL_BEHAVIOUR now unsupported → failEventReports"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [
                {
                    "event": "UE_COMMUNICATION",
                    "tgtUe": {"supis": ["imsi-208930000000003"]}
                },
                {
                    "event": "ABNORMAL_BEHAVIOUR",
                    "tgtUe": {"supis": ["imsi-123456789"]},
                    "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}]
                }
            ],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "201" ]; then
        HAS_FAIL=$(echo "$BODY" | jq 'has("failEventReports")')
        FAIL_EVENT=$(echo "$BODY" | jq -r '.failEventReports[0].event // empty')
        if [ "$HAS_FAIL" = "true" ] && [ "$FAIL_EVENT" = "ABNORMAL_BEHAVIOUR" ]; then
            log_success "Created (201) with ABNORMAL_BEHAVIOUR in failEventReports"
        else
            log_error "Should have ABNORMAL_BEHAVIOUR in failEventReports"
        fi
    else
        log_error "Expected 201 but returned HTTP $HTTP_CODE"
    fi
    echo "$BODY" | jq .
}

# Test 7: evtReq PERIODIC without repPeriod (should reject)
test_evtreq_periodic_invalid() {
    log_info "Test: evtReq PERIODIC without repPeriod (should reject)"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "UE_COMMUNICATION",
                "tgtUe": {"supis": ["imsi-208930000000003"]}
            }],
            "notificationURI": "http://localhost:9090/callback",
            "evtReq": {
                "notifMethod": "PERIODIC"
            }
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "400" ]; then
        log_success "Correctly rejected (400) - PERIODIC needs repPeriod"
    else
        log_error "Should reject but returned HTTP $HTTP_CODE"
    fi
    echo "$BODY" | jq .
}

# Test 8: Valid evtReq with PERIODIC + repPeriod
test_evtreq_valid() {
    log_info "Test: Valid evtReq with PERIODIC + repPeriod"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "UE_COMMUNICATION",
                "tgtUe": {"supis": ["imsi-208930000000003"]}
            }],
            "notificationURI": "http://localhost:9090/callback",
            "evtReq": {
                "notifMethod": "PERIODIC",
                "repPeriod": 60,
                "maxReportNbr": 10
            }
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "201" ]; then
        log_success "Created successfully (201) with evtReq"
        echo "$BODY" | jq .
    else
        log_error "Creation failed (HTTP $HTTP_CODE)"
        echo "$BODY" | jq .
    fi
}

# Test 9: Analytics target period (startTs in past + endTs in future)
test_target_period() {
    log_info "Test: Analytics target period - BOTH_STAT_PRED_NOT_ALLOWED"
    
    # Create dates: past (1 hour ago) and future (1 hour from now)
    PAST_TS=$(date -u -d "-1 hour" +"%Y-%m-%dT%H:%M:%SZ")
    FUTURE_TS=$(date -u -d "+1 hour" +"%Y-%m-%dT%H:%M:%SZ")
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "UE_COMMUNICATION",
                "tgtUe": {"supis": ["imsi-208930000000003"]},
                "extraReportReq": {
                    "startTs": "'"$PAST_TS"'",
                    "endTs": "'"$FUTURE_TS"'"
                }
            }],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "400" ]; then
        CAUSE=$(echo "$BODY" | jq -r '.cause // empty')
        if [ "$CAUSE" = "BOTH_STAT_PRED_NOT_ALLOWED" ]; then
            log_success "Correctly rejected (400) with cause BOTH_STAT_PRED_NOT_ALLOWED"
        else
            log_error "Wrong cause: $CAUSE (expected BOTH_STAT_PRED_NOT_ALLOWED)"
        fi
    else
        log_error "Expected 400, got HTTP $HTTP_CODE"
    fi
    echo "$BODY" | jq .
}

# Test: Delete subscription
test_delete() {
    if [ -z "$1" ]; then
        log_error "subscriptionId is required"
        return
    fi
    
    log_info "Test: Delete subscription $1"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X DELETE "${BASE_URL}/subscriptions/$1")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    
    if [ "$HTTP_CODE" = "204" ]; then
        log_success "Deleted successfully (204)"
    else
        log_error "Delete failed (HTTP $HTTP_CODE)"
    fi
}

# Run all tests
run_all() {
    echo "========================================"
    echo "  NWDAF EventSubscription API Tests"
    echo "  Supported Event: UE_COMMUNICATION"
    echo "========================================"
    echo ""
    
    echo "--- UE_COMMUNICATION Validation Tests ---"
    test_create_valid
    echo ""
    test_ue_comm_intgroupids
    echo ""
    test_ue_comm_missing_tgtue
    echo ""
    test_ue_comm_missing_identifiers
    echo ""
    
    echo "--- Event Type Tests ---"
    test_unsupported_event
    echo ""
    test_abnormal_now_unsupported
    echo ""
    
    echo "--- evtReq & Target Period Tests ---"
    test_evtreq_periodic_invalid
    echo ""
    test_evtreq_valid
    echo ""
    test_target_period
    echo ""
    
    echo "========================================"
    echo "  Tests Completed"
    echo "========================================"
}

# Main
case "$1" in
    create)
        test_create_valid
        ;;
    intgroup)
        test_ue_comm_intgroupids
        ;;
    missing-tgtue)
        test_ue_comm_missing_tgtue
        ;;
    missing-id)
        test_ue_comm_missing_identifiers
        ;;
    unsupported)
        test_unsupported_event
        ;;
    abnormal)
        test_abnormal_now_unsupported
        ;;
    evtreq)
        test_evtreq_periodic_invalid
        ;;
    evtreq-valid)
        test_evtreq_valid
        ;;
    target_period)
        test_target_period
        ;;
    delete)
        test_delete "$2"
        ;;
    all|"")
        run_all
        ;;
    *)
        echo "Usage: $0 [create|intgroup|missing-tgtue|missing-id|unsupported|abnormal|evtreq|evtreq-valid|target_period|delete <id>|all]"
        exit 1
        ;;
esac
