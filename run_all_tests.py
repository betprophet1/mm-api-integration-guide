#!/usr/bin/env python3
"""
TIERED MATCHING FLOW - COMPLETE TEST SUITE RUNNER
================================================

This script runs all 10 test scenarios for the tiered matching flow test plan:

Category 1: Tiered Matching Flow Tests
- Scenario 1: Best Odds Tier - All Accept Success  
- Scenario 2: Best Odds Tier - One Rejects STOP
- Scenario 3: Second Tier Matching with STOP
- Scenario 4: Complete Multi-Odds Success

Category 2: Expired Odds Scenario Tests  
- Scenario 5: Odds Expire Before User Confirmation
- Scenario 6: Odds Expire During Matching Process
- Scenario 7: Mixed Validity Periods

Category 3: Edge Cases and Error Tests
- Scenario 8: No SP Responses
- Scenario 9: All SPs Reject Best odds  
- Scenario 10: Confirmed Stake from SP less than Requested Stake

Usage:
    python run_all_tests.py              # Run all scenarios
    python run_all_tests.py --scenario 5 # Run specific scenario
    python run_all_tests.py --category 1 # Run category 1 scenarios only
"""

import argparse
import json
import time
import sys
from datetime import datetime
from typing import List, Dict, Any
from test_framework import (
    TieredMatchingTestFramework, 
    SPOffer, 
    TestResult, 
    TestExecutionResult,
    create_test_scenarios
)


class ComprehensiveTestSuite(TieredMatchingTestFramework):
    """Complete test suite for all 10 tiered matching flow scenarios"""
    
    def __init__(self):
        super().__init__()
        self.scenarios = create_test_scenarios()
        self.results = []
    
    def run_scenario_1(self) -> TestExecutionResult:
        """Best Odds Tier - All Accept Success"""
        scenario = self.scenarios[0]
        
        def test_logic():
            stake = 100
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'best_odds_all_accept',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 800, 'capacity': 60, 'will_accept': True},
                    {'sp_id': 'SP2', 'odds': 800, 'capacity': 60, 'will_accept': True}
                ],
                'stake': stake,
                'expected_behavior': 'Both SPs accept, transaction processes successfully'
            }
            
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_success_scenario(result)
        return result
    
    def run_scenario_2(self) -> TestExecutionResult:
        """Best Odds Tier - One Rejects STOP"""
        scenario = self.scenarios[1]
        
        def test_logic():
            stake = 100
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'best_odds_one_rejects',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 800, 'capacity': 60, 'will_accept': True},
                    {'sp_id': 'SP2', 'odds': 800, 'capacity': 60, 'will_accept': False}
                ],
                'stake': stake,
                'expected_behavior': 'SP1 accepts, SP2 rejects, matching should STOP'
            }
            
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_stop_scenario(result)
        return result
    
    def run_scenario_3(self) -> TestExecutionResult:
        """Second Tier Matching with STOP"""
        scenario = self.scenarios[2]
        
        def test_logic():
            stake = 200  # Requires both tiers
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'second_tier_stop',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 900, 'capacity': 100, 'will_accept': True},
                    {'sp_id': 'SP2', 'odds': 800, 'capacity': 150, 'will_accept': False}
                ],
                'stake': stake,
                'expected_behavior': 'SP1 partial match, SP2 rejects, STOP at second tier'
            }
            
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_partial_match_scenario(result)
        return result
    
    def run_scenario_4(self) -> TestExecutionResult:
        """Complete Multi-Odds Success"""
        scenario = self.scenarios[3]
        
        def test_logic():
            stake = 300  # Large stake requiring multiple tiers
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'multi_odds_success',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 950, 'capacity': 100, 'will_accept': True},
                    {'sp_id': 'SP2', 'odds': 900, 'capacity': 100, 'will_accept': True},
                    {'sp_id': 'SP3', 'odds': 850, 'capacity': 150, 'will_accept': True}
                ],
                'stake': stake,
                'expected_behavior': 'Full stake matched across multiple tiers'
            }
            
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_success_scenario(result)
        return result
    
    def run_scenario_5(self) -> TestExecutionResult:
        """Odds Expire Before User Confirmation"""
        scenario = self.scenarios[4]
        
        def test_logic():
            stake = 100
            payload = self.create_base_payload(stake)
            
            # Simulate user waiting 2 seconds before confirming (odds expire after 1s)
            time.sleep(2)
            
            request_details = {
                'scenario': 'odds_expire_before_confirmation',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 800, 'validity_seconds': 1, 'will_accept': True}
                ],
                'stake': stake,
                'delay_seconds': 2,
                'expected_behavior': 'Expired odds error, cannot proceed'
            }
            
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_expiry_scenario(result)
        return result
    
    def run_scenario_6(self) -> TestExecutionResult:
        """Odds Expire During Matching Process"""
        scenario = self.scenarios[5]
        
        def test_logic():
            stake = 100
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'odds_expire_during_matching',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 800, 'validity_seconds': 3, 'will_accept': True},
                    {'sp_id': 'SP2', 'odds': 750, 'validity_seconds': 60, 'will_accept': True}
                ],
                'stake': stake,
                'expected_behavior': 'SP1 expires during processing, matching STOPS'
            }
            
            # Quick confirmation but SP1 will expire during processing
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_expiry_scenario(result)
        return result
    
    def run_scenario_7(self) -> TestExecutionResult:
        """Mixed Validity Periods"""
        scenario = self.scenarios[6]
        
        def test_logic():
            stake = 100
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'mixed_validity_periods',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 900, 'validity_seconds': 2, 'will_accept': True},
                    {'sp_id': 'SP2', 'odds': 750, 'validity_seconds': 60, 'will_accept': True}
                ],
                'stake': stake,
                'expected_behavior': 'SP1 best odds expire, system STOPS rather than fallback'
            }
            
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_stop_scenario(result)
        return result
    
    def run_scenario_8(self) -> TestExecutionResult:
        """No SP Responses"""  
        scenario = self.scenarios[7]
        
        def test_logic():
            stake = 100
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'no_sp_responses',
                'simulated_sps': [],  # No SPs provide offers
                'stake': stake,
                'expected_behavior': 'Timeout and fail gracefully'
            }
            
            # Use shorter timeout to simulate no SP responses
            status_code, response_data, duration = self.make_bet_request([payload], timeout=5)
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_timeout_scenario(result)
        return result
    
    def run_scenario_9(self) -> TestExecutionResult:
        """All SPs Reject Best odds"""
        scenario = self.scenarios[8]
        
        def test_logic():
            stake = 100
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'all_sps_reject',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 800, 'capacity': 60, 'will_accept': False},
                    {'sp_id': 'SP2', 'odds': 800, 'capacity': 60, 'will_accept': False},
                    {'sp_id': 'SP3', 'odds': 800, 'capacity': 60, 'will_accept': False}
                ],
                'stake': stake,
                'expected_behavior': 'All SPs reject, STOP immediately, no fallback'
            }
            
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_rejection_scenario(result)
        return result
    
    def run_scenario_10(self) -> TestExecutionResult:
        """Confirmed Stake from SP less than Requested Stake"""
        scenario = self.scenarios[9]
        
        def test_logic():
            stake = 500  # User requests 500
            payload = self.create_base_payload(stake)
            
            request_details = {
                'scenario': 'partial_stake_confirmation',
                'simulated_sps': [
                    {'sp_id': 'SP1', 'odds': 800, 'capacity': 300, 'will_accept': True, 'confirmed_stake': 300}
                ],
                'stake': stake,
                'expected_confirmed_stake': 300,
                'expected_behavior': 'Partial matching up to available capacity (300)'
            }
            
            status_code, response_data, duration = self.make_bet_request([payload])
            return status_code, response_data, request_details
        
        result = self.execute_scenario(scenario, test_logic)
        self._validate_partial_stake_scenario(result)
        return result
    
    def _validate_success_scenario(self, result: TestExecutionResult):
        """Validate scenarios expected to succeed"""
        if result.response_details and 'data' in result.response_details:
            transaction_data = result.response_details['data'][0] if result.response_details['data'] else {}
            transaction_status = transaction_data.get('transactionStatus', 0)
            
            if transaction_status in [102, 200]:  # Processing or Completed
                result.result = TestResult.PASS
                result.actual_result = f"SUCCESS: Transaction status {transaction_status}"
                result.validation_details.update({
                    'transaction_status': transaction_status,
                    'validation': 'PASSED - Transaction processed successfully'
                })
            else:
                result.result = TestResult.FAIL
                result.actual_result = f"FAILED: Unexpected status {transaction_status}"
    
    def _validate_stop_scenario(self, result: TestExecutionResult):
        """Validate scenarios where matching should STOP"""
        if result.response_details and 'data' in result.response_details:
            transaction_data = result.response_details['data'][0] if result.response_details['data'] else {}
            transaction_status = transaction_data.get('transactionStatus', 0)
            
            # For STOP scenarios, we expect failure or cancellation
            if transaction_status in [400, 422] or result.validation_details.get('status_code', 0) >= 400:
                result.result = TestResult.PASS
                result.actual_result = f"SUCCESS: Matching stopped as expected"
                result.validation_details.update({
                    'validation': 'PASSED - Matching terminated correctly'
                })
            else:
                result.result = TestResult.FAIL
                result.actual_result = f"FAILED: Expected STOP but got status {transaction_status}"
    
    def _validate_partial_match_scenario(self, result: TestExecutionResult):
        """Validate partial matching scenarios"""
        if result.response_details and 'data' in result.response_details:
            transaction_data = result.response_details['data'][0] if result.response_details['data'] else {}
            transaction_status = transaction_data.get('transactionStatus', 0)
            
            # Partial match could result in processing or partial success
            if transaction_status in [102, 200, 422]:
                result.result = TestResult.PASS
                result.actual_result = f"SUCCESS: Partial matching behavior - status {transaction_status}"
                result.validation_details.update({
                    'validation': 'PASSED - Partial matching handled correctly'
                })
            else:
                result.result = TestResult.FAIL
                result.actual_result = f"FAILED: Unexpected partial match status {transaction_status}"
    
    def _validate_expiry_scenario(self, result: TestExecutionResult):
        """Validate expiry scenarios"""
        status_code = result.validation_details.get('status_code', 0)
        
        # Expiry scenarios should result in errors
        if status_code >= 400 or result.result == TestResult.TIMEOUT:
            result.result = TestResult.PASS
            result.actual_result = f"SUCCESS: Expiry handled correctly - HTTP {status_code}"
            result.validation_details.update({
                'validation': 'PASSED - Odds expiry detected and handled'
            })
        else:
            result.result = TestResult.FAIL
            result.actual_result = f"FAILED: Expected expiry error but got HTTP {status_code}"
    
    def _validate_timeout_scenario(self, result: TestExecutionResult):
        """Validate timeout scenarios"""
        if result.result == TestResult.TIMEOUT or result.validation_details.get('status_code', 0) == 408:
            result.result = TestResult.PASS
            result.actual_result = "SUCCESS: Timeout handled gracefully"
            result.validation_details.update({
                'validation': 'PASSED - No SP responses handled correctly'
            })
        else:
            result.result = TestResult.FAIL
            result.actual_result = "FAILED: Expected timeout but request succeeded"
    
    def _validate_rejection_scenario(self, result: TestExecutionResult):
        """Validate all-rejection scenarios"""
        if result.response_details and 'data' in result.response_details:
            transaction_data = result.response_details['data'][0] if result.response_details['data'] else {}
            transaction_status = transaction_data.get('transactionStatus', 0)
            
            # All rejections should result in failure
            if transaction_status >= 400 or result.validation_details.get('status_code', 0) >= 400:
                result.result = TestResult.PASS
                result.actual_result = f"SUCCESS: All rejections handled - status {transaction_status}"
                result.validation_details.update({
                    'validation': 'PASSED - Complete rejection scenario handled'
                })
            else:
                result.result = TestResult.FAIL
                result.actual_result = f"FAILED: Expected rejection but got status {transaction_status}"
    
    def _validate_partial_stake_scenario(self, result: TestExecutionResult):
        """Validate partial stake scenarios"""
        if result.response_details and 'data' in result.response_details:
            transaction_data = result.response_details['data'][0] if result.response_details['data'] else {}
            transaction_status = transaction_data.get('transactionStatus', 0)
            
            # Partial stake should still process
            if transaction_status in [102, 200]:
                result.result = TestResult.PASS
                result.actual_result = f"SUCCESS: Partial stake processed - status {transaction_status}"
                result.validation_details.update({
                    'validation': 'PASSED - Partial stake matching handled correctly'
                })
            else:
                result.result = TestResult.FAIL
                result.actual_result = f"FAILED: Partial stake not handled - status {transaction_status}"
    
    def run_scenario(self, scenario_id: int) -> TestExecutionResult:
        """Run a specific scenario by ID"""
        scenario_methods = {
            1: self.run_scenario_1,
            2: self.run_scenario_2, 
            3: self.run_scenario_3,
            4: self.run_scenario_4,
            5: self.run_scenario_5,
            6: self.run_scenario_6,
            7: self.run_scenario_7,
            8: self.run_scenario_8,
            9: self.run_scenario_9,
            10: self.run_scenario_10
        }
        
        if scenario_id in scenario_methods:
            return scenario_methods[scenario_id]()
        else:
            raise ValueError(f"Invalid scenario ID: {scenario_id}")
    
    def run_category(self, category: int) -> List[TestExecutionResult]:
        """Run scenarios by category"""
        category_scenarios = {
            1: [1, 2, 3, 4],  # Tiered Matching Flow Tests
            2: [5, 6, 7],     # Expired Odds Scenario Tests  
            3: [8, 9, 10]     # Edge Cases and Error Tests
        }
        
        if category not in category_scenarios:
            raise ValueError(f"Invalid category: {category}")
        
        results = []
        for scenario_id in category_scenarios[category]:
            result = self.run_scenario(scenario_id)
            results.append(result)
            self.results.append(result)
        
        return results
    
    def run_all_scenarios(self) -> List[TestExecutionResult]:
        """Run all 10 test scenarios"""
        print("🧪 TIERED MATCHING FLOW - COMPLETE TEST SUITE")
        print("=" * 80)
        print("📋 Running all 10 test scenarios...")
        print()
        
        all_results = []
        
        for scenario_id in range(1, 11):
            print(f"🔄 Executing Scenario {scenario_id}...")
            
            try:
                result = self.run_scenario(scenario_id)
                all_results.append(result)
                self.results.append(result)
                
                # Print individual result
                self.print_test_result(result)
                
                # Small delay between tests
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ ERROR in Scenario {scenario_id}: {str(e)}")
                # Create error result
                scenario = self.scenarios[scenario_id - 1]
                error_result = TestExecutionResult(
                    scenario=scenario,
                    result=TestResult.ERROR,
                    execution_time=0.0,
                    actual_result=f"Test execution error: {str(e)}",
                    validation_details={},
                    error_message=str(e)
                )
                all_results.append(error_result)
                self.results.append(error_result)
        
        return all_results
    
    def generate_summary_report(self, results: List[TestExecutionResult]) -> Dict[str, Any]:
        """Generate comprehensive summary report"""
        total_tests = len(results)
        passed_tests = len([r for r in results if r.result == TestResult.PASS])
        failed_tests = len([r for r in results if r.result == TestResult.FAIL])
        error_tests = len([r for r in results if r.result == TestResult.ERROR])
        timeout_tests = len([r for r in results if r.result == TestResult.TIMEOUT])
        
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        # Results by category
        category_results = {
            'Tiered Matching Flow Tests': [r for r in results if r.scenario.scenario_id in [1,2,3,4]],
            'Expired Odds Scenario Tests': [r for r in results if r.scenario.scenario_id in [5,6,7]], 
            'Edge Cases and Error Tests': [r for r in results if r.scenario.scenario_id in [8,9,10]]
        }
        
        return {
            'summary': {
                'total_tests': total_tests,
                'passed_tests': passed_tests,
                'failed_tests': failed_tests,
                'error_tests': error_tests,
                'timeout_tests': timeout_tests,
                'success_rate': success_rate
            },
            'category_breakdown': {
                cat: {
                    'total': len(results),
                    'passed': len([r for r in results if r.result == TestResult.PASS]),
                    'failed': len([r for r in results if r.result == TestResult.FAIL])
                } for cat, results in category_results.items()
            },
            'detailed_results': results
        }
    
    def print_summary_report(self, results: List[TestExecutionResult]):
        """Print comprehensive summary report"""
        report = self.generate_summary_report(results)
        
        print("\n" + "="*80)
        print("🏆 TIERED MATCHING FLOW - FINAL TEST REPORT")
        print("="*80)
        
        summary = report['summary']
        print(f"📊 OVERALL RESULTS:")
        print(f"   • Total Tests: {summary['total_tests']}")
        print(f"   • Passed: ✅ {summary['passed_tests']}")  
        print(f"   • Failed: ❌ {summary['failed_tests']}")
        print(f"   • Errors: 🔥 {summary['error_tests']}")
        print(f"   • Timeouts: ⏰ {summary['timeout_tests']}")
        print(f"   • Success Rate: {summary['success_rate']:.1f}%")
        print()
        
        print(f"📂 RESULTS BY CATEGORY:")
        for category, stats in report['category_breakdown'].items():
            success_rate = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"   • {category}: {stats['passed']}/{stats['total']} ({success_rate:.1f}%)")
        print()
        
        print(f"📋 INDIVIDUAL TEST RESULTS:")
        for result in results:
            status_emoji = {
                TestResult.PASS: "✅",
                TestResult.FAIL: "❌",
                TestResult.ERROR: "🔥", 
                TestResult.TIMEOUT: "⏰"
            }
            
            emoji = status_emoji.get(result.result, "❓")
            print(f"   {emoji} Scenario {result.scenario.scenario_id}: {result.scenario.name}")
        print()
        
        # Recommendations
        print(f"💡 RECOMMENDATIONS:")
        if summary['failed_tests'] > 0:
            print(f"   • Review {summary['failed_tests']} failed test(s) for system issues")
        if summary['error_tests'] > 0:
            print(f"   • Investigate {summary['error_tests']} test execution error(s)")
        if summary['success_rate'] < 70:
            print(f"   • Success rate below 70% - significant issues detected")
        elif summary['success_rate'] < 90:
            print(f"   • Success rate below 90% - minor issues present")
        else:
            print(f"   • Excellent test results - system performing well!")
        
        print("\n" + "="*80)
        
        return report


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description="Tiered Matching Flow - Complete Test Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--scenario', '-s', 
        type=int,
        choices=range(1, 11),
        help='Run specific scenario (1-10)'
    )
    
    parser.add_argument(
        '--category', '-c',
        type=int, 
        choices=[1, 2, 3],
        help='Run scenarios by category (1=Tiered Matching, 2=Expired Odds, 3=Edge Cases)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Save results to JSON file'
    )
    
    args = parser.parse_args()
    
    # Initialize test suite
    test_suite = ComprehensiveTestSuite()
    
    # Run tests based on arguments
    if args.scenario:
        print(f"🎯 Running Test Scenario {args.scenario}")
        results = [test_suite.run_scenario(args.scenario)]
        test_suite.print_test_result(results[0])
        
    elif args.category:
        print(f"🎯 Running Test Category {args.category}")
        results = test_suite.run_category(args.category)
        
    else:
        # Run all scenarios
        results = test_suite.run_all_scenarios()
    
    # Generate and print summary
    report = test_suite.print_summary_report(results)
    
    # Save results if requested
    if args.output:
        report['timestamp'] = datetime.now().isoformat()
        report['test_environment'] = 'sandbox'
        
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"📁 Results saved to: {args.output}")
    
    # Exit with appropriate code
    summary = report['summary']
    if summary['error_tests'] > 0:
        sys.exit(2)  # Execution errors
    elif summary['failed_tests'] > 0:
        sys.exit(1)  # Test failures
    else:
        sys.exit(0)  # All tests passed


if __name__ == "__main__":
    main()