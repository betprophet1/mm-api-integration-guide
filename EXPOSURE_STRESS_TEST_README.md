# Exposure & Stress Testing Suite

## Overview
This repository contains automated testing scripts for Market Maker (MM) API exposure generation and stress testing. The suite includes coordinated multi-account betting scripts designed to test exposure credit (GEC/LEC) generation and system performance under load.

---

## 📋 Table of Contents
- [Scripts Overview](#scripts-overview)
- [Script Details](#script-details)
- [Function Context](#function-context)
- [Environment Configuration](#environment-configuration)
- [Usage Guidelines](#usage-guidelines)
- [Performance Reports](#performance-reports)
- [Troubleshooting](#troubleshooting)

---

## Scripts Overview

### 1. **exposure-autoplay.py**
**Purpose:** Automated coordinated betting between two accounts to generate exposure credits (GEC/LEC)

**Type:** Functional Testing

**Key Features:**
- Dual authentication (MM API + Web API)
- Coordinated opposing bet placement
- Real-time GEC/LEC monitoring
- Automatic event rotation
- Conservative betting approach

**Use Case:** Daily functional testing to verify exposure generation mechanisms work correctly

---

### 2. **exposure-stress-test.py**
**Purpose:** High-frequency multi-threaded stress testing to evaluate system performance under heavy load

**Type:** Load/Stress Testing

**Key Features:**
- Multi-worker parallel execution
- Configurable load targets
- Performance metrics tracking
- Automatic report generation
- Error handling and recovery
- Real-time progress monitoring

**Use Case:** Performance validation, capacity planning, and system stability testing under high transaction volumes

---

### 3. **stress_test_30k.py**
**Purpose:** Aggressive single-event stress test placing and canceling 30,000 wagers

**Type:** Extreme Load Testing

**Key Features:**
- Batch wager placement (20 per API call)
- Immediate wager cancellation
- High-concurrency worker threads
- Rate limiting protection
- Progress tracking

**Use Case:** Testing API throughput limits and wager lifecycle at extreme scale

---

### 4. **daily_stress_test.py**
**Purpose:** Multi-account coordinated stress testing for daily automation

**Type:** Scheduled Load Testing

**Key Features:**
- Multiple iterations with configurable duration
- Aggressive coordinated betting
- Event rotation
- Comprehensive logging
- Production-ready error handling

**Use Case:** Daily automated testing in CI/CD pipelines or scheduled test runs

---

## Script Details

### exposure-autoplay.py

#### Configuration
```python
BASE_URL = "https://api-ss-sandbox.betprophet.co"
TARGET_EVENTS = 3  # Number of events to generate GEC from
MAX_BET_ROUNDS = 20  # Maximum betting rounds per event
```

#### How It Works
1. **Authentication Phase**: Logs in both accounts using MM and Web APIs
2. **Seeding Phase**: Account 1 discovers tournaments, events, and markets
3. **Coordinated Betting**: Alternates bet placement between accounts on opposing outcomes
4. **Exposure Monitoring**: Checks for GEC/LEC generation after each betting round
5. **Event Rotation**: Moves to next event after GEC is generated

#### Command Line
```bash
# Run with default settings
python exposure-autoplay.py

# No command line arguments (hardcoded configuration)
```

#### Output
- Real-time bet placement logs
- GEC/LEC generation notifications
- Balance changes
- Final summary report

---

### exposure-stress-test.py

#### Configuration
```bash
--target       # Number of bets to place (default: 1000)
--gec-events   # Target GEC events (default: 5)
--workers      # Number of concurrent workers (default: 5)
--env          # Environment: sandbox/staging (default: sandbox)
```

#### How It Works
1. **Initialize**: Authenticate both accounts on specified environment
2. **Seed Markets**: Discover available events and moneyline markets
3. **Spawn Workers**: Create worker threads for parallel bet placement
4. **Coordinated Betting**: Each worker places opposing bets between accounts
5. **Monitor Progress**: Real-time stats every 10 seconds
6. **Generate Report**: Creates comprehensive performance report on completion

#### Command Line Examples
```bash
# Basic run with defaults (1000 bets, 5 workers, sandbox)
python exposure-stress-test.py

# Custom configuration
python exposure-stress-test.py --target 2000 --workers 10 --gec-events 10

# Staging environment test
python exposure-stress-test.py --env staging --target 500 --workers 3

# Quick test
python exposure-stress-test.py --target 100 --workers 2
```

#### Output
- Worker startup logs
- Bet placement success/failure messages
- Progress updates every 10 seconds
- Final statistics box
- Performance report file: `exposure_stress_test_report_YYYYMMDD_HHMMSS.txt`

---

### stress_test_30k.py

#### Configuration
```bash
--event     # Target event name (required)
--workers   # Number of concurrent workers (default: 10)
--target    # Target number of wagers (default: 30000)
--env       # Environment: sandbox/staging (default: sandbox)
```

#### How It Works
1. **Login**: Authenticate using MM API
2. **Find Event**: Search for specified event by name
3. **Select Market**: Choose market with valid selections
4. **Batch Placement**: Workers place wagers in batches of 20
5. **Immediate Cancellation**: Cancel wagers as they accumulate
6. **Progress Monitoring**: Real-time stats every 5 seconds

#### Command Line Examples
```bash
# Place 30k wagers on specific event
python stress_test_30k.py --event "Lakers vs Warriors"

# Custom target with more workers
python stress_test_30k.py --event "Red Sox" --target 50000 --workers 20

# Sandbox environment (default)
python stress_test_30k.py --event "Cubs" --env sandbox

# Staging environment
python stress_test_30k.py --event "Yankees" --env staging --target 10000
```

#### Output
- Event discovery logs
- Progress reports every 5 seconds (placement/cancellation rates)
- Final summary with timing and balance

---

### daily_stress_test.py

#### Configuration
```bash
--env          # Environment: sandbox/production (required)
--accounts     # Account numbers to use (comma-separated or count)
--duration     # Duration per iteration in seconds (default: 300)
--iterations   # Number of test iterations (default: 5)
```

#### How It Works
1. **Multi-Account Setup**: Initialize specified accounts
2. **Event Rotation**: Background thread continuously rotates target events
3. **Coordinated Betting**: All accounts bet on same events simultaneously
4. **Iteration Loop**: Runs for specified number of iterations
5. **Statistics Tracking**: Per-account and aggregate metrics
6. **Logging**: Comprehensive logs saved to timestamped file

#### Command Line Examples
```bash
# Run with 2 accounts for 5 minutes per iteration
python daily_stress_test.py --env sandbox --accounts 2 --duration 300 --iterations 5

# Use specific accounts
python daily_stress_test.py --env sandbox --accounts 1,2,3 --duration 600 --iterations 3

# Quick test
python daily_stress_test.py --env sandbox --accounts 2 --duration 60 --iterations 1

# Production environment
python daily_stress_test.py --env production --accounts 1 --duration 900 --iterations 10
```

#### Output
- Log file: `daily_stress_test_YYYYMMDD_HHMMSS.log`
- Real-time console output
- Per-iteration statistics
- Final aggregate summary

---

## Environment Configuration

### Supported Environments

#### Sandbox (Default)
```
URL: https://api-ss-sandbox.betprophet.co
Purpose: Development and testing
Rate Limits: Relaxed
Data: Test data, can be reset
```

#### Staging
```
URL: https://api-ss-staging.betprophet.co
Purpose: Pre-production validation
Rate Limits: Production-like
Data: Staging data, more stable
```

### Account Credentials

Scripts use hardcoded test accounts. For production use, credentials should be:
- Stored in environment variables
- Loaded from secure configuration files
- Never committed to version control

---

## Usage Guidelines

### Best Practices

#### 1. **Start Small**
```bash
# Begin with low targets to verify setup
python exposure-stress-test.py --target 50 --workers 2
```

#### 2. **Monitor Progress**
- Watch for error messages
- Check progress reports
- Verify bet placement is working

#### 3. **Gradual Scale-Up**
```bash
# Increase load gradually
python exposure-stress-test.py --target 100 --workers 3
python exposure-stress-test.py --target 500 --workers 5
python exposure-stress-test.py --target 2000 --workers 10
```

#### 4. **Environment Considerations**
- Use **sandbox** for development and testing
- Use **staging** for pre-production validation
- Always test in sandbox before staging

#### 5. **Graceful Shutdown**
- Use `Ctrl+C` to stop tests gracefully
- Scripts handle interrupts and save progress
- Check final reports even after interruption

---

### When to Use Each Script

| Script | Use Case | Frequency | Load Level |
|--------|----------|-----------|------------|
| **exposure-autoplay.py** | Functional testing of GEC/LEC generation | Daily | Low (20-40 bets) |
| **exposure-stress-test.py** | Performance testing, system validation | Weekly/On-demand | Medium-High (100-5000 bets) |
| **stress_test_30k.py** | Extreme load testing, throughput limits | Monthly/Before releases | Very High (10k-50k wagers) |
| **daily_stress_test.py** | Automated daily testing, CI/CD | Daily (automated) | Medium (configurable) |

---

## Performance Reports

### exposure-stress-test.py Report Sections

#### 1. Test Configuration
- Environment
- Target metrics
- Worker count
- Test duration

#### 2. Results Summary
- Total bets placed
- Success rate
- GEC events generated
- Average throughput

#### 3. Worker Performance
- Individual worker statistics
- Bets per worker
- Error counts
- Performance rates

#### 4. Account Statistics
- Starting balances
- Ending balances
- Balance changes

#### 5. Error Analysis
- Total error count
- Error breakdown by type
- Recent error log

#### 6. Performance Metrics
- Bets per second/minute
- Success rate percentage
- Error rate percentage

### Report Example
```
================================================================================
EXPOSURE STRESS TEST - PERFORMANCE REPORT
================================================================================

Test Date: 2025-12-12 05:15:30
Environment: sandbox
Duration: 185.43 seconds (3.09 minutes)

================================================================================
TEST CONFIGURATION
================================================================================
Target Bets: 1000
Target GEC Events: 5
Number of Workers: 5
Number of Accounts: 2

================================================================================
RESULTS SUMMARY
================================================================================
Total Bets Placed: 1000 / 1000 (100.0%)
GEC Events Generated: 3 / 5
Total Errors: 12
Average Bet Rate: 5.39 bets/second
Peak Bet Rate: 6.72 bets/second

================================================================================
WORKER PERFORMANCE
================================================================================
  worker_1:
    Bets Placed: 200
    Successful Pairs: 100
    Errors: 2
    Duration: 182.45s
    Bet Rate: 1.10 bets/s
    
  [Additional workers...]

================================================================================
ACCOUNT STATISTICS
================================================================================

Account 1:
  Starting Balance: $10000.00
  Ending Balance: $9975.50
  Balance Change: $-24.50

Account 2:
  Starting Balance: $10000.00
  Ending Balance: $10024.50
  Balance Change: $24.50
```

---

## Troubleshooting

### Common Issues

#### 1. **Test Hangs at Low Bet Count**

**Symptom:** Progress stops after 10-20 bets

**Causes:**
- No opposing outcomes available (outcome_id 4 and 5 missing)
- Market has insufficient selections
- API rate limiting

**Solutions:**
```bash
# Reduce workers to decrease API load
python exposure-stress-test.py --workers 2 --target 100

# Check script output for specific errors
# Look for "Cannot find opposing outcomes" messages
```

#### 2. **Authentication Failures**

**Symptom:** "❌ Authentication failed" messages

**Causes:**
- Invalid credentials
- Wrong environment
- Network issues

**Solutions:**
- Verify account credentials in script
- Check environment URL is accessible
- Test with `curl` or browser first

#### 3. **High Error Rate**

**Symptom:** Many bet placement failures

**Causes:**
- Insufficient account balance
- Invalid line IDs
- Market closed/unavailable

**Solutions:**
- Check account balances before running
- Verify markets are open
- Review error log in performance report

#### 4. **No Markets Found**

**Symptom:** "❌ No market data available"

**Causes:**
- No active tournaments
- No events with moneyline markets
- Wrong tournament configuration

**Solutions:**
- Check tournament availability in environment
- Modify market type filters if needed
- Run during active sports seasons

#### 5. **Rate Limiting**

**Symptom:** HTTP 429 errors or slow progress

**Causes:**
- Too many concurrent requests
- API rate limits exceeded

**Solutions:**
```bash
# Reduce worker count
python exposure-stress-test.py --workers 2

# Increase delays in worker code
# Add time.sleep() between operations
```

---

## Performance Tips

### Optimizing Test Execution

#### 1. **Worker Count**
- Start with 2-3 workers
- Increase gradually based on success rate
- Monitor error rates as you scale

#### 2. **Target Setting**
```bash
# Quick validation (1-2 minutes)
--target 100 --workers 2

# Medium test (5-10 minutes)
--target 500 --workers 5

# Full stress test (20+ minutes)
--target 2000 --workers 10
```

#### 3. **Environment Selection**
- **Sandbox**: Best for development and rapid iteration
- **Staging**: Use for final validation before production

#### 4. **Monitoring**
- Watch console output during execution
- Check progress reports every 10 seconds
- Review detailed report after completion

---

## Testing Checklist

### Pre-Test
- [ ] Verify account credentials are correct
- [ ] Check environment is accessible
- [ ] Confirm sufficient account balances
- [ ] Verify markets/events are available
- [ ] Set appropriate target and worker count

### During Test
- [ ] Monitor console output for errors
- [ ] Check progress reports for stuck workers
- [ ] Verify bet placement is succeeding
- [ ] Watch for rate limiting warnings

### Post-Test
- [ ] Review performance report
- [ ] Check account balances
- [ ] Analyze error logs if any
- [ ] Document any issues encountered
- [ ] Archive reports for historical tracking

---

## Future Enhancements

### Planned Improvements
1. **Configuration Files**: Move credentials to external config
2. **Database Integration**: Store test results in database
3. **Grafana Dashboards**: Real-time performance visualization
4. **Alerting**: Automated notifications on failures
5. **Test Scheduling**: Cron-based automated execution
6. **Market Type Flexibility**: Support spread and total markets
7. **Dynamic Odds**: Vary odds instead of fixed 160/-160

---

## Support & Contact

For issues, questions, or enhancements:
- Review error logs and performance reports
- Check troubleshooting section above
- Contact the development team with report files

---

## Appendix

### Quick Command Reference

```bash
# Exposure Autoplay (Functional Testing)
python exposure-autoplay.py

# Exposure Stress Test (Performance Testing)
python exposure-stress-test.py --target 1000 --workers 5 --env sandbox

# 30K Stress Test (Extreme Load)
python stress_test_30k.py --event "EventName" --target 30000 --workers 10 --env sandbox

# Daily Stress Test (Automated Testing)
python daily_stress_test.py --env sandbox --accounts 2 --duration 300 --iterations 5
```

### File Outputs

| Script | Output File Pattern | Content |
|--------|-------------------|---------|
| exposure-stress-test.py | `exposure_stress_test_report_YYYYMMDD_HHMMSS.txt` | Performance report |
| daily_stress_test.py | `daily_stress_test_YYYYMMDD_HHMMSS.log` | Execution logs |

---

**Last Updated:** 2025-12-12  
**Version:** 1.0
