#!/usr/bin/env python3
"""
30-Minute Extended Exposure Autoplay Test
Runs exposure-autoplay continuously for 30 minutes and tracks metrics
"""

import subprocess
import time
import json
from datetime import datetime, timedelta

# Test configuration
TEST_DURATION_MINUTES = 30
REPORT_FILE = f"autoplay_30min_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

def run_test():
    """Run the 30-minute exposure autoplay test"""
    
    print("="*80)
    print("🚀 30-MINUTE EXPOSURE AUTOPLAY TEST")
    print("="*80)
    print(f"Start Time: {datetime.now()}")
    print(f"Duration: {TEST_DURATION_MINUTES} minutes")
    print(f"Report File: {REPORT_FILE}")
    print("="*80)
    print()
    
    # Metrics tracking
    metrics = {
        'start_time': datetime.now(),
        'test_runs': 0,
        'successful_runs': 0,
        'failed_runs': 0,
        'total_gec_events': 0,
        'total_bets': 0,
        'errors': [],
        'run_details': []
    }
    
    end_time = datetime.now() + timedelta(minutes=TEST_DURATION_MINUTES)
    
    while datetime.now() < end_time:
        run_number = metrics['test_runs'] + 1
        run_start = datetime.now()
        
        print(f"\n{'='*80}")
        print(f"🔄 RUN #{run_number} - {run_start.strftime('%H:%M:%S')}")
        print(f"⏱️  Time remaining: {(end_time - datetime.now()).total_seconds() / 60:.1f} minutes")
        print(f"{'='*80}\n")
        
        try:
            # Run exposure-autoplay.py
            result = subprocess.run(
                ['python3', 'exposure-autoplay.py'],
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout per run
            )
            
            output = result.stdout + result.stderr
            
            # Parse output for metrics
            gec_count = output.count('GEC GENERATED')
            bet_count = output.count('Bet placed successfully')
            success = 'SUCCESS!' in output
            
            run_duration = (datetime.now() - run_start).total_seconds()
            
            run_detail = {
                'run': run_number,
                'start_time': run_start.strftime('%H:%M:%S'),
                'duration': run_duration,
                'success': success,
                'gec_events': gec_count,
                'bets_placed': bet_count
            }
            
            metrics['test_runs'] += 1
            metrics['run_details'].append(run_detail)
            
            if success:
                metrics['successful_runs'] += 1
                metrics['total_gec_events'] += gec_count
                metrics['total_bets'] += bet_count
                print(f"✅ Run #{run_number} completed: {gec_count} GEC events, {bet_count} bets")
            else:
                metrics['failed_runs'] += 1
                print(f"⚠️  Run #{run_number} had issues")
                
        except subprocess.TimeoutExpired:
            metrics['test_runs'] += 1
            metrics['failed_runs'] += 1
            metrics['errors'].append(f"Run {run_number}: Timeout after 10 minutes")
            print(f"❌ Run #{run_number} timed out")
            
        except Exception as e:
            metrics['test_runs'] += 1
            metrics['failed_runs'] += 1
            metrics['errors'].append(f"Run {run_number}: {str(e)}")
            print(f"❌ Run #{run_number} error: {e}")
        
        # Small delay between runs
        if datetime.now() < end_time:
            print("\n⏸️  Waiting 10 seconds before next run...")
            time.sleep(10)
    
    # Calculate final metrics
    metrics['end_time'] = datetime.now()
    metrics['total_duration'] = (metrics['end_time'] - metrics['start_time']).total_seconds() / 60
    
    # Generate report
    generate_report(metrics)
    
    return metrics

def generate_report(metrics):
    """Generate detailed test report"""
    
    report_lines = []
    report_lines.append("="*80)
    report_lines.append("30-MINUTE EXPOSURE AUTOPLAY TEST - FINAL REPORT")
    report_lines.append("="*80)
    report_lines.append("")
    report_lines.append(f"Test Date: {metrics['start_time'].strftime('%Y-%m-%d')}")
    report_lines.append(f"Start Time: {metrics['start_time'].strftime('%H:%M:%S')}")
    report_lines.append(f"End Time: {metrics['end_time'].strftime('%H:%M:%S')}")
    report_lines.append(f"Total Duration: {metrics['total_duration']:.2f} minutes")
    report_lines.append("")
    report_lines.append("="*80)
    report_lines.append("OVERALL METRICS")
    report_lines.append("="*80)
    report_lines.append(f"Total Test Runs: {metrics['test_runs']}")
    report_lines.append(f"Successful Runs: {metrics['successful_runs']}")
    report_lines.append(f"Failed Runs: {metrics['failed_runs']}")
    report_lines.append(f"Success Rate: {(metrics['successful_runs']/max(metrics['test_runs'],1)*100):.1f}%")
    report_lines.append("")
    report_lines.append(f"Total GEC Events Generated: {metrics['total_gec_events']}")
    report_lines.append(f"Total Bets Placed: {metrics['total_bets']}")
    
    if metrics['test_runs'] > 0:
        report_lines.append(f"Avg GEC Events per Run: {metrics['total_gec_events']/metrics['test_runs']:.1f}")
        report_lines.append(f"Avg Bets per Run: {metrics['total_bets']/metrics['test_runs']:.1f}")
    
    report_lines.append("")
    report_lines.append("="*80)
    report_lines.append("RUN DETAILS")
    report_lines.append("="*80)
    
    for detail in metrics['run_details']:
        status = "✅" if detail['success'] else "❌"
        report_lines.append(f"{status} Run #{detail['run']} @ {detail['start_time']} - "
                          f"Duration: {detail['duration']:.1f}s, "
                          f"GEC: {detail['gec_events']}, "
                          f"Bets: {detail['bets_placed']}")
    
    if metrics['errors']:
        report_lines.append("")
        report_lines.append("="*80)
        report_lines.append("ERRORS")
        report_lines.append("="*80)
        for error in metrics['errors']:
            report_lines.append(f"❌ {error}")
    
    report_lines.append("")
    report_lines.append("="*80)
    report_lines.append("END OF REPORT")
    report_lines.append("="*80)
    
    # Write to file
    report_content = "\n".join(report_lines)
    with open(REPORT_FILE, 'w') as f:
        f.write(report_content)
    
    # Print to console
    print("\n" + report_content)
    print(f"\n📄 Report saved to: {REPORT_FILE}")

if __name__ == "__main__":
    try:
        metrics = run_test()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Test failed with error: {e}")
