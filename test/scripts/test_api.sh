#!/bin/bash
# NWDAF API Test Script
# Usage: ./test_api.sh [start|create|update|delete|all]

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

# Test 1: Create valid subscription
test_create_valid() {
    log_info "Test: Create valid subscription (ABNORMAL_BEHAVIOUR + DDOS)"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "ABNORMAL_BEHAVIOUR",
                "tgtUe": {"anyUe": true},
                "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
                "dnns": ["internet"]
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

# Test 2: Mutual exclusion validation (excepRequs + exptAnaType)
test_mutual_exclusion() {
    log_info "Test: excepRequs and exptAnaType mutual exclusion"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "ABNORMAL_BEHAVIOUR",
                "tgtUe": {"anyUe": true},
                "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
                "exptAnaType": "COMMUN"
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

# Test 3: anyUe missing required fields
test_anyue_missing_fields() {
    log_info "Test: anyUe=true missing dnns/networkArea"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "ABNORMAL_BEHAVIOUR",
                "tgtUe": {"anyUe": true},
                "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}]
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

# Test 4: Unsupported event type
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

# Test 5: evtReq PERIODIC without repPeriod (Phase 2B)
test_evtreq_periodic() {
    log_info "Test: evtReq PERIODIC without repPeriod (should reject)"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "ABNORMAL_BEHAVIOUR",
                "tgtUe": {"anyUe": true},
                "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
                "dnns": ["internet"]
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

# Test 6: Mixed events - one supported event, one unsupported event → 201 + failEventReports
test_unsupported_exception() {
    log_info "Test: Mixed events (ABNORMAL_BEHAVIOUR + UE_MOBILITY) → 201 + failEventReports"
    
    # Send 2 events: one supported (ABNORMAL_BEHAVIOUR), one unsupported (UE_MOBILITY)
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [
                {
                    "event": "ABNORMAL_BEHAVIOUR",
                    "tgtUe": {"supis": ["imsi-123456789"]},
                    "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}]
                },
                {
                    "event": "UE_MOBILITY",
                    "tgtUe": {"supis": ["imsi-987654321"]}
                }
            ],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    # Returns 201 with failEventReports for the unsupported event type
    if [ "$HTTP_CODE" = "201" ]; then
        HAS_FAIL=$(echo "$BODY" | jq 'has("failEventReports")')
        if [ "$HAS_FAIL" = "true" ]; then
            log_success "Created (201) with failEventReports for unsupported event"
        else
            log_error "Should have failEventReports but missing"
        fi
    else
        log_error "Expected 201 but returned HTTP $HTTP_CODE"
    fi
    echo "$BODY" | jq .
}

# Test 7: Unsupported exptAnaType → 400 UNSUPPORTED_ANALYTICS_TYPE
test_unsupported_anatype() {
    log_info "Test: Unsupported exptAnaType (MOBILITY) → 400 UNSUPPORTED_ANALYTICS_TYPE"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "ABNORMAL_BEHAVIOUR",
                "tgtUe": {"supis": ["imsi-123456789"]},
                "exptAnaType": "MOBILITY",
                "networkArea": {"tais": [{"plmnId": {"mcc": "466", "mnc": "01"}, "tac": "1234"}]}
            }],
            "notificationURI": "http://localhost:9090/callback"
        }')
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    # Unsupported exptAnaType → 400 rejection
    if [ "$HTTP_CODE" = "400" ]; then
        CAUSE=$(echo "$BODY" | jq -r '.cause')
        if [ "$CAUSE" = "UNSUPPORTED_ANALYTICS_TYPE" ]; then
            log_success "Correctly rejected (400) - UNSUPPORTED_ANALYTICS_TYPE"
        else
            log_error "Expected cause UNSUPPORTED_ANALYTICS_TYPE but got $CAUSE"
        fi
    else
        log_error "Expected 400 but returned HTTP $HTTP_CODE"
    fi
    echo "$BODY" | jq .
}

# Test 8: Valid evtReq with PERIODIC and repPeriod (Phase 2B)
test_evtreq_valid() {
    log_info "Test: Valid evtReq with PERIODIC + repPeriod"
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "ABNORMAL_BEHAVIOUR",
                "tgtUe": {"anyUe": true},
                "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
                "dnns": ["internet"]
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

# Test 10: Analytics target period (startTs in past + endTs in future)
test_target_period() {
    log_info "Test: Analytics target period - BOTH_STAT_PRED_NOT_ALLOWED"
    
    # Create dates: past (1 hour ago) and future (1 hour from now)
    PAST_TS=$(date -u -d "-1 hour" +"%Y-%m-%dT%H:%M:%SZ")
    FUTURE_TS=$(date -u -d "+1 hour" +"%Y-%m-%dT%H:%M:%SZ")
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/subscriptions" \
        -H "Content-Type: application/json" \
        -d '{
            "eventSubscriptions": [{
                "event": "ABNORMAL_BEHAVIOUR",
                "tgtUe": {"anyUe": true},
                "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
                "dnns": ["internet"],
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

# Run all tests
run_all() {
    echo "========================================"
    echo "  NWDAF EventSubscription API Tests"
    echo "========================================"
    echo ""
    
    echo "--- Basic Validation Tests ---"
    test_create_valid
    echo ""
    test_mutual_exclusion
    echo ""
    test_anyue_missing_fields
    echo ""
    test_unsupported_event
    echo ""
    
    echo "--- Phase 2B/2C Validation Tests ---"
    test_evtreq_periodic
    echo ""
    test_unsupported_exception
    echo ""
    test_unsupported_anatype
    echo ""
    test_evtreq_valid
    echo ""
    test_target_period
    echo ""
    
    echo "========================================"
    echo "  Tests Completed"
    echo "========================================"
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

# Main
case "$1" in
    create)
        test_create_valid
        ;;
    mutual)
        test_mutual_exclusion
        ;;
    anyue)
        test_anyue_missing_fields
        ;;
    unsupported)
        test_unsupported_event
        ;;
    # Phase 2B tests
    evtreq)
        test_evtreq_periodic
        ;;
    exception)
        test_unsupported_exception
        ;;
    anatype)
        test_unsupported_anatype
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
        echo "Usage: $0 [create|mutual|anyue|unsupported|evtreq|exception|anatype|evtreq-valid|target_period|delete <id>|all]"
        exit 1
        ;;
esac
