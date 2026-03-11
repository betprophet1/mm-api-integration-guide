# DEDUCE Feature - Pair Testing Checklist
**Date**: December 19, 2025  
**Environment**: Staging  
**Focus**: Patron <> Patron <> MM/SP with Exposure Management

## Test Accounts
- **Patron 1**: parlay_cash_patron@betprophet.co (Testing@123 / OTP: 123456)
- **Patron 2**: thinh.tran@betprophet.co (Matkhau1$)
- **Patron 3**: lam.tran+usr001@betprophet.co (Kh0ngbiet1)

---

## 🎯 Critical Path Tests (Must Complete)

### 1. Basic DEDUCE Flow - Patron to Patron Match
**Objective**: Verify money is NOT deducted on placement, only on match

**Test Steps**:
- [ ] **Setup**: Patron1 has $100 balance, Patron2 has $100 balance
- [ ] **Patron1**: Place $100 wager on Team A to WIN (back) at odds +150
- [ ] **Verify**: Patron1 balance still shows $100 (NO DEDUCTION)
- [ ] **Verify**: Wager status = "open/unmatched"
- [ ] **Patron2**: Place $100 wager on Team A to LOSE (lay) at compatible odds
- [ ] **Verify Match Occurs**: Wagers matched
- [ ] **Verify**: Patron1 balance NOW shows $0 (deducted after match)
- [ ] **Verify**: Patron2 balance NOW shows $0 (deducted after match)
- [ ] **Check**: Transaction logs show deduction timestamp = match timestamp

**Expected**: ✅ Balance deducted ONLY when matched, NOT on placement

---

### 2. Multiple Wagers with Same Balance (Core DEDUCE Feature)
**Objective**: Users can place multiple wagers with same $100 on different unique markets

**Test Steps**:
- [ ] **Setup**: Patron1 has $100 balance
- [ ] **Patron1**: Place $100 wager on NBA Game 1 - Lakers ML
- [ ] **Verify**: Balance still $100, wager open/unmatched
- [ ] **Patron1**: Place $100 wager on NFL Game 2 - Patriots ML  
- [ ] **Verify**: Balance still $100, both wagers open/unmatched
- [ ] **Patron1**: Place $100 wager on MLB Game 3 - Yankees ML
- [ ] **Verify**: Balance still $100, all 3 wagers open/unmatched
- [ ] **Check Exposure**: Max exposure = $100 (current balance), can support up to $1000 total potential (10x)
- [ ] **Patron2**: Match the NBA Lakers wager
- [ ] **Verify**: Patron1 balance drops to $0 (first wager matched)
- [ ] **Verify**: Other 2 wagers (NFL, MLB) auto-cancelled due to balance depletion
- [ ] **Check**: Patron1 cannot place new wagers (balance = $0)

**Expected**: ✅ Can place multiple wagers with same balance on unique markets, auto-cancel on balance depletion

---

### 3. Patron <> MM Match (Immediate Liquidity)
**Objective**: Patron wagers can match against MM (Market Maker)

**Test Steps**:
- [ ] **Setup**: MM has liquidity on Team B spread -3.5 at odds -110
- [ ] **Setup**: Patron1 has $200 balance
- [ ] **Patron1**: Place $100 wager on Team B spread -3.5 at odds -110
- [ ] **Verify**: Balance still $200 (no deduction on placement)
- [ ] **Verify Match**: Wager matched against MM liquidity immediately
- [ ] **Verify**: Patron1 balance drops to $100 after MM match
- [ ] **Check MM Position**: MM exposure updated correctly
- [ ] **Verify**: Transaction shows match type = "MM"

**Expected**: ✅ Patron-MM matches work correctly with deferred deduction

---

### 4. Patron <> SP (Sports Pool) Match
**Objective**: Patron wagers can match against SP liquidity

**Test Steps**:
- [ ] **Setup**: SP has open liquidity for specific market
- [ ] **Setup**: Patron1 has $150 balance
- [ ] **Patron1**: Place $150 wager targeting SP market
- [ ] **Verify**: Balance still $150 (no deduction)
- [ ] **Verify Match**: Wager matched against SP
- [ ] **Verify**: Patron1 balance drops to $0 after SP match
- [ ] **Check SP Position**: SP exposure updated correctly

**Expected**: ✅ Patron-SP matches work with DEDUCE timing

---

### 5. Partial Match Scenario
**Objective**: Partial matches deduct partial amounts correctly

**Test Steps**:
- [ ] **Setup**: Patron1 has $200 balance
- [ ] **Patron1**: Place $100 wager on Team C ML
- [ ] **Verify**: Balance still $200
- [ ] **Patron2**: Place $60 wager to match (partial)
- [ ] **Verify Partial Match**: $60 matched, $40 remains unmatched
- [ ] **Verify**: Patron1 balance drops to $140 ($200 - $60 matched portion)
- [ ] **Verify**: Wager shows $60 matched, $40 open/unmatched
- [ ] **Patron1**: Check if can place new wagers with remaining $140

**Expected**: ✅ Partial matches deduct only matched amounts

---

### 6. Auto-Cancellation After Balance Depletion
**Objective**: Remaining open wagers cancelled when balance becomes $0

**Test Steps**:
- [ ] **Setup**: Patron1 has $100 balance
- [ ] **Patron1**: Place 5 wagers of $100 each on different markets (Market A, B, C, D, E)
- [ ] **Verify**: All 5 wagers open, balance still $100
- [ ] **Check Exposure**: Total exposure $500, max allowed based on $100 balance
- [ ] **Trigger Match**: Patron2 matches wager on Market A ($100)
- [ ] **Verify**: Patron1 balance drops to $0
- [ ] **Verify Auto-Cancel**: Other 4 wagers (B, C, D, E) automatically cancelled
- [ ] **Check Notifications**: Patron1 receives cancellation notifications
- [ ] **Verify Status**: Market A = matched, Markets B-E = cancelled

**Expected**: ✅ Auto-cancellation prevents over-exposure

---

## 💰 Exposure Management Tests

### 7. Maximum Exposure Calculation (10x Rule)
**Objective**: System enforces max exposure = Balance × 10

**Test Steps**:
- [ ] **Setup**: Patron1 has $100 balance, exposure multiplier = 10x
- [ ] **Calculate**: Max allowed exposure = $100 × 10 = $1,000
- [ ] **Patron1**: Place 10 wagers of $100 each on different markets
- [ ] **Verify**: All 10 wagers accepted (total $1,000 exposure)
- [ ] **Patron1**: Attempt 11th wager of $100
- [ ] **Verify REJECTION**: System rejects 11th wager (exceeds max exposure)
- [ ] **Error Message**: "Maximum exposure limit exceeded"
- [ ] **Patron1**: Cancel 2 open wagers (reduces exposure to $800)
- [ ] **Patron1**: Attempt new $100 wager
- [ ] **Verify**: Now accepted (within $1,000 limit)

**Expected**: ✅ Exposure limit enforced correctly

---

### 8. Exposure with Partial Matches
**Objective**: Exposure calculation updates correctly with partial matches

**Test Steps**:
- [ ] **Setup**: Patron1 has $100 balance
- [ ] **Patron1**: Place 5 wagers × $100 = $500 total exposure
- [ ] **Patron2**: Partially match wager #1 for $60
- [ ] **Verify**: Wager #1 shows $60 matched, $40 open
- [ ] **Calculate Current Exposure**: 
  - Wager 1: $40 (remaining open)
  - Wagers 2-5: $100 each = $400
  - Total: $440 exposure
- [ ] **Verify**: Available exposure = $1,000 - $440 = $560
- [ ] **Patron1**: Can place new wagers up to $560 additional exposure
- [ ] **Patron1**: Attempt $600 new wager
- [ ] **Verify REJECTION**: Exceeds available exposure

**Expected**: ✅ Exposure recalculated correctly after partial matches

---

### 9. Exposure Across Multiple Markets
**Objective**: Exposure tracked correctly across different sport events

**Test Steps**:
- [ ] **Setup**: Patron1 has $100 balance
- [ ] **Patron1**: Place wagers on:
  - NBA Game 1: $100 (Lakers ML)
  - NFL Game 2: $100 (Patriots spread -3.5)
  - MLB Game 3: $100 (Yankees over 8.5)
  - Tennis Match 4: $100 (Federer ML)
  - Soccer Match 5: $100 (Barcelona ML)
- [ ] **Verify**: Total exposure = $500 across 5 different sports/markets
- [ ] **Verify**: All wagers show as unique markets (no conflicts)
- [ ] **Check Dashboard**: Exposure breakdown by sport
- [ ] **Trigger Match**: Lakers wager matches ($100)
- [ ] **Verify**: Balance = $0, other 4 wagers auto-cancelled
- [ ] **Verify**: Exposure drops to $0 (only matched wager remains)

**Expected**: ✅ Cross-market exposure tracking accurate

---

## 🔄 Concurrent & Edge Cases

### 10. Simultaneous Matches - Race Condition
**Objective**: System handles concurrent matches without over-deducting

**Test Steps**:
- [ ] **Setup**: Patron1 has $100 balance, 3 open wagers ($100 each)
- [ ] **Concurrent Action**: 
  - Patron2 matches wager A at same time
  - Patron3 matches wager B at same time
  - MM matches wager C at same time
- [ ] **Verify**: Only ONE wager matches successfully
- [ ] **Verify**: Balance deducted only once ($100 → $0)
- [ ] **Verify**: Other 2 match attempts fail gracefully
- [ ] **Check Logs**: Race condition handled correctly, no double-deduction
- [ ] **Verify**: Other unmatched wagers auto-cancelled

**Expected**: ✅ No race condition issues, atomic balance updates

---

### 11. Cancel Wager Before Match
**Objective**: User can cancel unmatched wagers, balance remains unchanged

**Test Steps**:
- [ ] **Setup**: Patron1 has $100 balance
- [ ] **Patron1**: Place $100 wager on Team D
- [ ] **Verify**: Balance still $100, wager open
- [ ] **Patron1**: Cancel the wager manually
- [ ] **Verify**: Wager status = "cancelled"
- [ ] **Verify**: Balance still $100 (nothing was deducted)
- [ ] **Verify**: Exposure reduced correctly

**Expected**: ✅ Cancel works without affecting balance

---

### 12. Mixed Balance Types (Cash + GEC + LEC)
**Objective**: DEDUCE works with mixed wallet types

**Test Steps**:
- [ ] **Setup**: Patron1 has $50 Cash + $50 GEC = $100 total
- [ ] **Patron1**: Place $100 wager on Team E
- [ ] **Verify**: Balance shows $100 available, wager open
- [ ] **Patron2**: Match the wager
- [ ] **Verify Deduction Order**: 
  - First: $50 deducted from Cash
  - Then: $50 deducted from GEC
- [ ] **Verify Final Balance**: Cash = $0, GEC = $0
- [ ] **Check Transaction Log**: Shows correct breakdown

**Expected**: ✅ Mixed balance types handled correctly

---

### 13. LEC (Line of Credit) Fallback
**Objective**: System uses LEC when Cash+GEC insufficient

**Test Steps**:
- [ ] **Setup**: Patron1 has $30 Cash + $20 GEC + $100 LEC
- [ ] **Patron1**: Place $80 wager
- [ ] **Verify**: Wager placed (within total available = $150)
- [ ] **Patron2**: Match the wager
- [ ] **Verify Deduction**:
  - $30 from Cash
  - $20 from GEC  
  - $30 from LEC
- [ ] **Verify**: Cash=$0, GEC=$0, LEC=$70 remaining

**Expected**: ✅ LEC used correctly as fallback

---

## 🚨 Error Scenarios

### 14. Insufficient Balance for Match
**Objective**: Match fails gracefully if balance becomes insufficient

**Test Steps**:
- [ ] **Setup**: Patron1 has $100, places 2 wagers ($100 each)
- [ ] **External Event**: Admin manually deducts $50 from Patron1 (balance = $50)
- [ ] **Patron2**: Attempts to match first $100 wager
- [ ] **Verify**: Match fails or only $50 matched
- [ ] **Verify**: Error handling graceful, no system crash
- [ ] **Check Notifications**: Patron1 notified of insufficient balance

**Expected**: ✅ Graceful failure handling

---

### 15. Zero Dollar Wager
**Objective**: System rejects $0 wagers

**Test Steps**:
- [ ] **Patron1**: Attempt to place $0 wager
- [ ] **Verify REJECTION**: Error message "Wager amount must be greater than $0"
- [ ] **Verify**: No wager created in system

**Expected**: ✅ Validation prevents $0 wagers

---

### 16. Negative Wager Amount
**Objective**: System rejects negative amounts

**Test Steps**:
- [ ] **Patron1**: Attempt to place -$50 wager
- [ ] **Verify REJECTION**: Validation error
- [ ] **Verify**: No database changes

**Expected**: ✅ Input validation works

---