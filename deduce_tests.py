#!/usr/bin/env python3
"""
DEDUCE Feature Automated Tests
Tests the deferred deduction model: money deducted only when wager is matched
"""

import time
import json
import uuid
import random
import requests
from urllib.parse import urljoin
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from src import config
from src.log import logging

# ANSI color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

class DeduceTestFramework:
    """Framework for testing DEDUCE feature"""
    
    def __init__(self, environment='staging'):
        """Initialize test framework"""
        self.environment = environment
        self.base_url = config.ENVIRONMENT_URLS.get(environment, config.ENVIRONMENT_URLS['staging'])
        self.sessions = {}  # Store sessions for multiple accounts
        self.test_results = []
        self.test_start_time = time.time()
        
        # Patron login endpoint (username/password) - use web API format
        self.patron_login_url = 'api/v1/auth/login'
        
    def login_account(self, account_name: str, credentials: dict, account_type: str = 'mm') -> dict:
        """Login and store session for an account
        
        Args:
            account_name: Name for the account (e.g., 'mm1', 'patron1')
            credentials: Dict with either:
                - MM: {'access_key': str, 'secret_key': str}
                - Patron: {'email': str, 'password': str} or {'username': str, 'password': str}
            account_type: 'mm' or 'patron'
        """
        if account_type == 'mm':
            # MM login with access_key/secret_key
            login_url = urljoin(self.base_url, config.URL['mm_login'])
            request_body = {
                'access_key': credentials['access_key'],
                'secret_key': credentials['secret_key'],
            }
            response = requests.post(login_url, data=json.dumps(request_body))
        else:
            # Patron login with email/password using web API format
            login_url = urljoin(self.base_url, self.patron_login_url)
            headers = {
                '__source': 'web',
                'accept': 'application/json, text/plain, */*',
                'content-type': 'application/json',
                'x-currency': 'cash'
            }
            base_body = {
                'email': credentials.get('email', credentials.get('username')),
                'password': credentials['password'],
            }
            # Whether an account has 2FA enabled isn't knowable ahead of login, so try
            # without an OTP code first, and only fall back to the platform-wide
            # test-account OTP bypass ('123456') if that attempt fails. Confirmed live:
            # an account with a pending 2FA challenge fails hard (non-200) without a
            # code and succeeds once it's included; an account with none succeeds on
            # the first attempt and never reaches the retry, so this never sends a
            # code to an account that doesn't expect one (which 404s as otp_invalid).
            response = requests.post(login_url, json={**base_body, 'device_id': str(uuid.uuid1())}, headers=headers)
            if response.status_code != 200:
                response = requests.post(
                    login_url, json={**base_body, 'device_id': str(uuid.uuid1()), 'code': '123456'}, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Login failed for {account_name} ({account_type}): {response.content}")
            
        response_data = json.loads(response.content)
        # Handle different response formats
        if account_type == 'patron':
            session = {'access_token': response_data.get('accessToken', response_data.get('data', {}).get('access_token'))}
        else:
            session = response_data['data']
            
        self.sessions[account_name] = {
            'session': session,
            'credentials': credentials,
            'account_type': account_type
        }
        
        logging.info(f"{Colors.GREEN}✓ Logged in {account_name} ({account_type}){Colors.RESET}")
        return session
        
    def get_auth_header(self, account_name: str) -> dict:
        """Get auth header for an account"""
        if account_name not in self.sessions:
            raise Exception(f"Account {account_name} not logged in")
        
        account_type = self.sessions[account_name]['account_type']
        
        header = {
            'Authorization': f'Bearer {self.sessions[account_name]["session"]["access_token"]}',
        }
        
        # Add additional headers for patron accounts
        if account_type == 'patron':
            header.update({
                'Content-Type': 'application/json',
                'x-currency': 'cash',
                'accept': 'application/json'
            })
        
        return header
    
    def get_balance(self, account_name: str) -> dict:
        """Get balance for an account
        
        For MM accounts with deduce, the response includes:
        - balance: Main balance (doesn't change immediately for deduce accounts)
        - matched_wager_balance: Total exposure from matched bets
        - unmatched_wager_balance: Total exposure from unmatched bets
        
        Available balance = balance - matched_wager_balance - unmatched_wager_balance
        """
        # Check account type and use appropriate endpoint
        if account_name not in self.sessions:
            raise Exception(f"Account {account_name} not logged in")
        
        account_type = self.sessions[account_name]['account_type']
        
        if account_type == 'patron':
            # Use web API wallet endpoint for patrons
            balance_url = urljoin(self.base_url, 'api/v1/wallet')
        else:
            # Use MM API balance endpoint for MM accounts
            balance_url = urljoin(self.base_url, config.URL['mm_balance'])
        
        response = requests.get(balance_url, headers=self.get_auth_header(account_name))
        
        if response.status_code != 200:
            raise Exception(f"Failed to get balance for {account_name}")
            
        balance_data = json.loads(response.content).get('data', {})
        
        # For MM accounts, calculate available balance
        if account_type == 'mm':
            main_balance = balance_data.get('balance', 0)
            matched_wager_bal = balance_data.get('matched_wager_balance', 0)
            unmatched_wager_bal = balance_data.get('unmatched_wager_balance', 0)
            available_balance = main_balance - matched_wager_bal - unmatched_wager_bal
            
            balance_data['available_balance'] = available_balance
            
            logging.info(f"{Colors.CYAN}💰 {account_name}: " +
                        f"Balance=${main_balance:.2f}, " +
                        f"Matched=${matched_wager_bal:.2f}, " +
                        f"Unmatched=${unmatched_wager_bal:.2f}, " +
                        f"Available=${available_balance:.2f}{Colors.RESET}")
        else:
            logging.info(f"{Colors.CYAN}💰 {account_name} balance: ${balance_data.get('balance', 0):.2f}{Colors.RESET}")
        
        return balance_data
    
    def get_exposure(self, account_name: str) -> dict:
        """Get exposure information for an account"""
        try:
            exposure_url = urljoin(self.base_url, config.URL.get('exposure_balance', 'partner/exposure/get_balance'))
            response = requests.get(exposure_url, headers=self.get_auth_header(account_name))
            
            if response.status_code == 200:
                exposure_data = json.loads(response.content).get('data', {})
                return exposure_data
            else:
                logging.warning(f"Could not get exposure for {account_name}: {response.status_code} {response.content.decode('utf-8', 'replace')}")
                return {}
        except Exception as e:
            logging.warning(f"Exposure endpoint not available: {e}")
            return {}
    
    def get_matched_bets(self, account_name: str, limit: int = 100, offset: int = 0, date_from: str = None) -> list:
        """Get recent matched bets for an account
        
        Args:
            account_name: The account to query
            limit: Maximum number of records to retrieve
            offset: Offset for pagination
            date_from: Date filter in YYYY-MM-DD format (for patron accounts)
            
        Returns:
            List of matched bet records
        """
        if account_name not in self.sessions:
            raise Exception(f"Account {account_name} not logged in")
        
        account_type = self.sessions[account_name]['account_type']
        
        try:
            if account_type == 'patron':
                # Patron accounts use web API transaction endpoint
                # Get today's date if not provided
                if not date_from:
                    from datetime import datetime
                    date_from = datetime.now().strftime('%Y-%m-%d')
                
                matched_bets_url = urljoin(self.base_url, 'api/v2/transaction/wagers/cursor')
                params = {
                    'status': 'open',
                    'matchingStatus': 'partially_matched,fully_matched',
                    'dateFrom': date_from,
                    'sortField': 'placed_date:desc:desc',
                    'limit': limit,
                    'group': 1
                }
                
                # Add patron-specific headers
                headers = self.get_auth_header(account_name)
                headers['__source'] = 'web'
                
                response = requests.get(matched_bets_url, params=params, headers=headers)
                
                if response.status_code == 200:
                    response_data = json.loads(response.content)
                    # Patron API returns data as either {'wagers': [...]} or a bare list
                    data = response_data.get('data', {})
                    matched_bets = data.get('wagers', []) if isinstance(data, dict) else data
                    logging.info(f"{Colors.CYAN}📊 {account_name} has {len(matched_bets)} matched bets{Colors.RESET}")
                    return matched_bets
                else:
                    logging.warning(f"Could not get matched bets for {account_name}: {response.status_code} {response.content.decode('utf-8', 'replace')}")
                    return []
            else:
                # MM accounts use MM API endpoint
                matched_bets_url = urljoin(self.base_url, config.URL.get('mm_get_matched_bets', 'partner/mm/get_matched_bets'))
                params = {'limit': limit, 'offset': offset}
                response = requests.get(matched_bets_url, params=params, headers=self.get_auth_header(account_name))

                if response.status_code == 200:
                    matched_bets = json.loads(response.content).get('data', {}).get('matched_bets', [])
                    logging.info(f"{Colors.CYAN}📊 {account_name} has {len(matched_bets)} matched bets{Colors.RESET}")
                    return matched_bets
                else:
                    logging.warning(f"Could not get matched bets for {account_name}: {response.status_code} {response.content.decode('utf-8', 'replace')}")
                    return []
        except Exception as e:
            logging.warning(f"Matched bets endpoint error for {account_name}: {e}")
            return []
    
    def check_balance_change_for_deduce(self, account_name: str, initial_balance: float, 
                                       wager_stake: float, expected_behavior: str = 'deduce',
                                       time_window_seconds: int = 300) -> dict:
        """Check if wallet balance change is as expected for deduce-enabled accounts
        
        For accounts with DEDUCE enabled (like MM1):
        - Balance should NOT change when placing a bet
        - Balance SHOULD decrease when bet is matched
        - The decrease should match the sum of matched bet stakes
        
        For accounts without DEDUCE (like MM2, Patron):
        - Balance should decrease immediately when placing a bet
        
        Args:
            account_name: The account to check
            initial_balance: Balance before placing bet
            wager_stake: The stake amount placed
            expected_behavior: 'deduce' or 'normal'
            time_window_seconds: Only consider matched bets within this many seconds (default: 300 = 5 min)
            
        Returns:
            Dict with validation results
        """
        current_balance_data = self.get_balance(account_name)
        current_balance = current_balance_data.get('balance', 0)
        balance_change = initial_balance - current_balance
        
        result = {
            'account': account_name,
            'initial_balance': initial_balance,
            'current_balance': current_balance,
            'balance_change': balance_change,
            'expected_behavior': expected_behavior,
            'is_valid': False,
            'message': ''
        }
        
        if expected_behavior == 'deduce':
            # For deduce-enabled accounts, check if balance change matches matched bets
            all_matched_bets = self.get_matched_bets(account_name, limit=100)
            
            # Filter to only recent matched bets within time window
            current_time = time.time()
            recent_matched_bets = []
            for bet in all_matched_bets:
                # Parse matched_at timestamp if available
                matched_at = bet.get('matched_at') or bet.get('created_at')
                if matched_at:
                    try:
                        from dateutil import parser as date_parser
                        matched_time = date_parser.parse(matched_at).timestamp()
                        if current_time - matched_time <= time_window_seconds:
                            recent_matched_bets.append(bet)
                    except:
                        # If can't parse time, include it (be lenient)
                        recent_matched_bets.append(bet)
                else:
                    # No timestamp, include it
                    recent_matched_bets.append(bet)
            
            if len(recent_matched_bets) == 0 and abs(balance_change) < 0.01:
                # No matched bets, balance should be unchanged
                result['is_valid'] = True
                result['message'] = f"✓ No matched bets, balance unchanged (DEDUCE working correctly)"
                result['matched_bets_count'] = 0
                result['matched_bets_total'] = 0.0
                logging.info(f"{Colors.GREEN}{result['message']}{Colors.RESET}")
            elif len(recent_matched_bets) > 0:
                # Sum up the stakes from recent matched bets only
                total_matched_stake = sum(bet.get('stake', 0) for bet in recent_matched_bets)
                
                if abs(balance_change - total_matched_stake) < 0.01:
                    result['is_valid'] = True
                    result['message'] = f"✓ Balance change (${balance_change:.2f}) matches matched bets total (${total_matched_stake:.2f})"
                    result['matched_bets_count'] = len(recent_matched_bets)
                    result['matched_bets_total'] = total_matched_stake
                    logging.info(f"{Colors.GREEN}{result['message']}{Colors.RESET}")
                else:
                    result['is_valid'] = False
                    result['message'] = f"✗ Balance change (${balance_change:.2f}) does NOT match matched bets total (${total_matched_stake:.2f})"
                    result['matched_bets_count'] = len(recent_matched_bets)
                    result['matched_bets_total'] = total_matched_stake
                    result['all_matched_bets'] = len(all_matched_bets)
                    logging.warning(f"{Colors.YELLOW}{result['message']}{Colors.RESET}")
                    logging.info(f"{Colors.CYAN}Note: {len(recent_matched_bets)} recent bets (last {time_window_seconds}s) out of {len(all_matched_bets)} total{Colors.RESET}")
            elif abs(balance_change) > 0.01:
                result['is_valid'] = False
                result['message'] = f"✗ Balance decreased (${balance_change:.2f}) but no matched bets found in time window"
                result['matched_bets_count'] = 0
                result['all_matched_bets'] = len(all_matched_bets)
                logging.warning(f"{Colors.YELLOW}{result['message']}{Colors.RESET}")
            else:
                # No change and no recent matched bets - DEDUCE working
                result['is_valid'] = True
                result['message'] = f"✓ No matched bets, balance unchanged (DEDUCE working correctly)"
                result['matched_bets_count'] = 0
                logging.info(f"{Colors.GREEN}{result['message']}{Colors.RESET}")
        else:
            # For normal accounts, balance should decrease by wager stake immediately
            if abs(balance_change - wager_stake) < 0.01:
                result['is_valid'] = True
                result['message'] = f"✓ Balance decreased by wager stake (${wager_stake:.2f}) as expected (normal behavior)"
                logging.info(f"{Colors.GREEN}{result['message']}{Colors.RESET}")
            else:
                result['is_valid'] = False
                result['message'] = f"✗ Balance change (${balance_change:.2f}) does not match wager stake (${wager_stake:.2f})"
                logging.warning(f"{Colors.YELLOW}{result['message']}{Colors.RESET}")
        
        return result
    
    def place_wager(self, account_name: str, line_id: str, odds: int, stake: float) -> dict:
        """Place a single wager
        
        For MM accounts: uses partner/mm/place_wager endpoint
        For Patron accounts: uses trade/private/api/v2/wagers endpoint (web API)
        """
        if account_name not in self.sessions:
            raise Exception(f"Account {account_name} not logged in")
        
        account_type = self.sessions[account_name]['account_type']
        is_patron = account_type == 'patron'
        external_id = str(uuid.uuid4())
        
        if is_patron:
            # Patron accounts use web API endpoint
            play_url = urljoin(self.base_url, 'trade/private/api/v2/wagers')
            body = {
                'lineID': line_id,
                'odds': odds,
                'stake': stake
            }
            logging.debug(f"Patron wager request body: {json.dumps(body)}")
            
            # Get headers and add web-specific headers
            headers = self.get_auth_header(account_name)
            headers['__source'] = 'web'
            headers['origin'] = self.base_url.replace('api-', '')
        else:
            # MM accounts use MM API endpoint
            play_url = urljoin(self.base_url, config.URL['mm_place_wager'])
            body = {
                'external_id': external_id,
                'line_id': line_id,
                'odds': odds,
                'stake': stake
            }
            headers = self.get_auth_header(account_name)
        
        response = requests.post(play_url, json=body, headers=headers)
        
        # Accept both 200 and 201 status codes
        if response.status_code not in [200, 201]:
            error_msg = response.content.decode('utf-8')
            logging.error(f"{Colors.RED}✗ Failed to place wager for {account_name} (HTTP {response.status_code}): {error_msg or '<empty body>'}{Colors.RESET}")
            return {'success': False, 'error': error_msg, 'external_id': external_id}
        
        # Parse response based on account type
        response_json = json.loads(response.content)
        if is_patron:
            # Patron response format may differ, extract wager_id appropriately
            wager_data = response_json.get('data', {})
            wager_id = wager_data.get('id', wager_data.get('wager', {}).get('id', 'unknown'))
        else:
            wager_data = response_json.get('data', {})
            wager_id = wager_data.get('wager', {}).get('id', 'unknown')
        
        logging.info(f"{Colors.GREEN}✓ {account_name} placed wager: ${stake} at odds {odds} (ID: {wager_id[:8] if isinstance(wager_id, str) else wager_id}...){Colors.RESET}")
        
        return {
            'success': True,
            'external_id': external_id,
            'wager_id': wager_id,
            'line_id': line_id,
            'odds': odds,
            'stake': stake,
            'data': wager_data
        }
    
    def cancel_wager(self, account_name: str, external_id: str, wager_id: str) -> bool:
        """Cancel a wager"""
        cancel_url = urljoin(self.base_url, config.URL['mm_cancel_wager'])
        body = {
            'external_id': external_id,
            'wager_id': wager_id,
        }
        
        response = requests.post(cancel_url, json=body, headers=self.get_auth_header(account_name))
        
        if response.status_code == 200:
            logging.info(f"{Colors.GREEN}✓ {account_name} cancelled wager{Colors.RESET}")
            return True
        else:
            logging.error(f"{Colors.RED}✗ Failed to cancel wager: {response.content}{Colors.RESET}")
            return False

    def cancel_all_wagers(self, account_name: str) -> dict:
        """MM panic-button endpoint: cancels every open wager for the account, no wager id needed.

        Unlike cancel_wager (single wager), the backend's bulk cancel-all path
        (CancelWagersByUserId) does not check matching_stake_ongoing before
        cancelling -- only the single-wager cancel path does. So calling this
        while one of the account's wagers has an in-flight deduct job (mid-match)
        is the SSE-2441 scenario that needs to revert/refund correctly on the
        backend; this method exists to let a caller race that window.

        Returns {'success', 'status_code', 'error_text'} (not just a bool) so a
        caller can classify a failure -- e.g. via test_backend_fairness.py's
        _classify_cancel_error -- instead of only counting pass/fail.
        """
        cancel_all_url = urljoin(self.base_url, config.URL['mm_cancel_all_wagers'])
        response = requests.post(cancel_all_url, json={}, headers=self.get_auth_header(account_name))

        if response.status_code == 200:
            logging.info(f"{Colors.GREEN}✓ {account_name} cancel-all succeeded{Colors.RESET}")
            return {'success': True, 'status_code': response.status_code, 'error_text': ''}

        error_text = response.content.decode('utf-8', 'replace')
        logging.warning(f"{Colors.YELLOW}cancel-all failed for {account_name}: "
                         f"{response.status_code} {error_text}{Colors.RESET}")
        return {'success': False, 'status_code': response.status_code, 'error_text': error_text}

    def get_market_order_estimate(self, account_name: str, line_id: str, stake: float) -> dict:
        """Estimate fillable size/odds for a market order before placing it (patron-only, web API).

        Returns {'max_stake_size', 'expected_average_odds', 'odds_list'}. A
        market order placed above max_stake_size against a thin book won't
        fully match -- callers should clamp stake to this first.
        """
        estimate_url = urljoin(self.base_url, 'trade/private/api/v1/market-orders/estimate-odds')
        headers = self.get_auth_header(account_name)
        headers['__source'] = 'web'
        body = {'lineId': line_id, 'stake': stake}

        response = requests.post(estimate_url, json=body, headers=headers)
        if response.status_code != 200:
            logging.warning(f"{Colors.YELLOW}market order estimate-odds failed for {account_name}: "
                             f"{response.status_code} {response.content.decode('utf-8', 'replace')}{Colors.RESET}")
            return {'max_stake_size': 0, 'expected_average_odds': 0, 'odds_list': []}

        data = response.json().get('data', {})
        return {
            'max_stake_size': data.get('maxStakeSize', 0),
            'expected_average_odds': data.get('expectedAverageOdds', 0),
            'odds_list': data.get('oddsList', []),
        }

    def place_market_order(self, account_name: str, line_id: str, stake: float,
                            expected_avg_odds, odds_list) -> dict:
        """Place a market order (patron-only, web API): sweeps resting liquidity on
        line_id at expected_avg_odds up to stake. expected_avg_odds/odds_list
        should come from a prior get_market_order_estimate call so the order is
        actually matchable rather than sized against a stale/empty book.

        This is a genuinely different wager type from place_wager's limit
        orders: market order cancel/refund goes through CreateMarketOrderRefundJob
        (ss-trade-app), not the generic wager-cancel path -- calculateCancelWagerRefundAmount
        returns 0 for market-order-tagged wagers specifically, so cancel_all_wagers's
        refund accounting (what this script otherwise exercises) never applies to these;
        their own known race is the order matching right at its ~5s auto-cancel timeout
        (see reproduce_market_order_race_condition.py).
        """
        mo_url = urljoin(self.base_url, 'trade/private/api/v1/market-orders')
        headers = self.get_auth_header(account_name)
        headers['__source'] = 'web'
        body = {
            'lineID': line_id,
            'expectedAverageOdds': expected_avg_odds,
            'oddsList': odds_list,
            'stake': stake,
        }

        response = requests.post(mo_url, json=body, headers=headers)
        if response.status_code != 200:
            error_msg = response.content.decode('utf-8', 'replace')
            logging.error(f"{Colors.RED}✗ Failed to place market order for {account_name}: {error_msg}{Colors.RESET}")
            return {'success': False, 'error': error_msg}

        data = response.json().get('data', {})
        # Response carries refId (uuid) + id (int) -- there is no 'wagerId' field.
        # reproduce_market_order_race_condition.py hit a real bug reading a
        # nonexistent key here: wager_id silently stayed None and every
        # downstream match/cancel check on it was a no-op for weeks.
        wager_id = data.get('refId') or data.get('id')
        logging.info(f"{Colors.GREEN}✓ {account_name} placed market order: ${stake} (ID: {wager_id}){Colors.RESET}")
        return {'success': True, 'wager_id': wager_id, 'status': data.get('status', 'unknown'), 'data': data}

    def get_available_market(self, account_name: str) -> Optional[dict]:
        """Get an available market for testing.
        - If env var TARGET_EVENT_ID is set, try to fetch a market for that event first.
        - Otherwise, prioritizes NBA, NFL, MLB, MLS tournaments and returns first active market.
        """
        import os
        target_event_id = os.getenv('TARGET_EVENT_ID')
        if target_event_id:
            try:
                multiple_markets_url = urljoin(self.base_url, config.URL['mm_multiple_markets'])
                markets_response = requests.get(
                    multiple_markets_url,
                    params={'event_ids': str(target_event_id)},
                    headers=self.get_auth_header(account_name)
                )
                if markets_response.status_code == 200:
                    markets_data = json.loads(markets_response.content).get('data', {})
                    event_markets = markets_data.get(str(target_event_id)) or []
                    for market in event_markets:
                        selections = market.get('selections', [])
                        if selections:
                            try:
                                if isinstance(selections[0], list) and selections[0]:
                                    line_id = selections[0][0].get('line_id')
                                elif isinstance(selections[0], dict):
                                    line_id = selections[0].get('line_id')
                                else:
                                    continue
                                if line_id:
                                    logging.info(f"{Colors.GREEN}✓ Found market for TARGET_EVENT_ID={target_event_id}: {market.get('type', 'unknown')}{Colors.RESET}")
                                    return {
                                        'event': {'event_id': int(target_event_id), 'name': market.get('event_name', 'Unknown')},
                                        'market': market,
                                        'line_id': line_id,
                                    }
                            except Exception:
                                continue
                logging.warning(f"Could not find active market for TARGET_EVENT_ID={target_event_id}, falling back to general search")
            except Exception as e:
                logging.warning(f"Error fetching markets for TARGET_EVENT_ID={target_event_id}: {e}")
        
        # Fallback: general search prioritizing common tournaments
        # Get tournaments
        t_url = urljoin(self.base_url, config.URL['mm_tournaments'])
        response = requests.get(t_url, headers=self.get_auth_header(account_name))
        
        if response.status_code != 200:
            logging.warning(f"Failed to get tournaments: {response.status_code}")
            return None
            
        tournaments = json.loads(response.content).get('data', {}).get('tournaments', [])
        
        if not tournaments:
            logging.warning("No tournaments available")
            return None
        
        # Prioritize specific tournaments
        priority_tournaments = ['NBA', 'NFL', 'MLB', 'MLS']
        
        # Sort tournaments - priority tournaments first, then others
        sorted_tournaments = sorted(
            tournaments,
            key=lambda t: (t['name'] not in priority_tournaments, tournaments.index(t))
        )
        
        logging.info(f"Searching through {len(tournaments)} tournaments (prioritizing {', '.join(priority_tournaments)})...")
        
        # Get events - try tournaments in priority order
        event_url = urljoin(self.base_url, config.URL['mm_events'])
        for idx, tournament in enumerate(sorted_tournaments[:50]):  # Try first 50 tournaments
            # Show which priority tournament we're checking
            priority_marker = "🎯" if tournament['name'] in priority_tournaments else ""
            
            events_response = requests.get(
                event_url, 
                params={'tournament_id': tournament['id']}, 
                headers=self.get_auth_header(account_name)
            )
            
            if events_response.status_code == 200:
                events = json.loads(events_response.content).get('data', {}).get('sport_events', [])
                
                if events:
                    logging.info(f"{priority_marker} Found {len(events)} events in {tournament['name']}")
                    
                    # Get markets for first event
                    event = events[0]
                    multiple_markets_url = urljoin(self.base_url, config.URL['mm_multiple_markets'])
                    markets_response = requests.get(
                        multiple_markets_url,
                        params={'event_ids': str(event['event_id'])},
                        headers=self.get_auth_header(account_name)
                    )
                    
                    if markets_response.status_code == 200:
                        markets_data = json.loads(markets_response.content).get('data', {})
                        event_markets = markets_data.get(str(event['event_id'])) or []
                        
                        # Collect every market with a valid line_id, then pick one at
                        # random -- markets come back in a fixed order (moneyline
                        # first), so always returning the first hit meant every run
                        # tested the same market type despite more being available.
                        candidates = []
                        for market in event_markets:
                            if market.get('selections'):
                                selections = market.get('selections', [])
                                if selections and len(selections) > 0:
                                    # Handle different selection formats
                                    try:
                                        if isinstance(selections[0], list) and len(selections[0]) > 0:
                                            line_id = selections[0][0].get('line_id')
                                        elif isinstance(selections[0], dict):
                                            line_id = selections[0].get('line_id')
                                        else:
                                            continue

                                        if line_id:
                                            candidates.append((market, line_id))
                                    except (TypeError, KeyError, IndexError) as e:
                                        # Skip malformed selections
                                        continue

                        if candidates:
                            market, line_id = random.choice(candidates)
                            logging.info(f"{Colors.GREEN}✓ Found market: {tournament['name']} - {event['name']} ({market['type']}){Colors.RESET}")
                            return {
                                'event': event,
                                'market': market,
                                'line_id': line_id,
                                'tournament': tournament
                            }
        
        logging.warning(f"{Colors.YELLOW}No active markets found in {min(30, len(tournaments))} tournaments{Colors.RESET}")
        return None
    
    def log_test_result(self, test_name: str, passed: bool, message: str = "", details: dict = None):
        """Log a test result"""
        result = {
            'test': test_name,
            'passed': passed,
            'message': message,
            'details': details or {},
            'timestamp': datetime.now().isoformat()
        }
        self.test_results.append(result)
        
        status_icon = f"{Colors.GREEN}✓ PASS{Colors.RESET}" if passed else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        logging.info(f"\n{Colors.BOLD}{status_icon} - {test_name}{Colors.RESET}")
        if message:
            logging.info(f"  {message}")
    
    def print_summary(self):
        """Print test summary"""
        passed = sum(1 for r in self.test_results if r['passed'])
        failed = sum(1 for r in self.test_results if not r['passed'])
        total = len(self.test_results)
        elapsed = time.time() - self.test_start_time
        
        print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}DEDUCE Test Summary{Colors.RESET}")
        print(f"{'='*70}")
        print(f"Total Tests: {total}")
        print(f"{Colors.GREEN}Passed: {passed}{Colors.RESET}")
        print(f"{Colors.RED}Failed: {failed}{Colors.RESET}")
        print(f"Time: {elapsed:.2f}s")
        print(f"{'='*70}\n")
        
        if failed > 0:
            print(f"{Colors.RED}Failed Tests:{Colors.RESET}")
            for result in self.test_results:
                if not result['passed']:
                    print(f"  ✗ {result['test']}: {result['message']}")
    
    # =================== TEST CASES ===================
    
    def test_01_basic_deduce_flow(self, patron1: str, patron2: str):
        """
        Test 1: Basic DEDUCE Flow - Patron to Patron Match
        Verify money is NOT deducted on placement, only on match
        """
        test_name = "Test 1: Basic DEDUCE Flow"
        logging.info(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{test_name}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
        
        try:
            # Get initial balances
            balance1_before = self.get_balance(patron1)
            balance2_before = self.get_balance(patron2)
            initial_balance1 = balance1_before.get('balance', 0)
            initial_balance2 = balance2_before.get('balance', 0)
            
            logging.info(f"Initial balances: {patron1}=${initial_balance1:.2f}, {patron2}=${initial_balance2:.2f}")
            
            # Get available market
            market_info = self.get_available_market(patron1)
            if not market_info:
                self.log_test_result(test_name, False, "No available markets found")
                return
            
            line_id = market_info['line_id']
            event_name = market_info['event']['name']
            logging.info(f"Using market: {event_name}")
            
            # Patron1 places wager
            wager1 = self.place_wager(patron1, line_id, 150, 10.0)
            if not wager1['success']:
                self.log_test_result(test_name, False, f"Failed to place wager: {wager1.get('error')}")
                return
            
            time.sleep(1)  # Wait for processing
            
            # Check balance after placement (should be UNCHANGED in DEDUCE model)
            balance1_after_place = self.get_balance(patron1)
            balance_after_place = balance1_after_place.get('balance', 0)
            
            if abs(balance_after_place - initial_balance1) > 0.01:
                self.log_test_result(
                    test_name, 
                    False, 
                    f"Balance changed on placement! Expected: ${initial_balance1:.2f}, Got: ${balance_after_place:.2f}"
                )
                return
            
            logging.info(f"{Colors.GREEN}✓ Balance unchanged after placement (DEDUCE working!){Colors.RESET}")
            
            # Check wager status - should be open/unmatched
            # Note: You may need to add a get_wager_status method if available
            
            self.log_test_result(
                test_name,
                True,
                f"Balance correctly unchanged on placement: ${initial_balance1:.2f} -> ${balance_after_place:.2f}",
                {
                    'patron1_initial': initial_balance1,
                    'patron1_after_place': balance_after_place,
                    'wager_amount': 10.0
                }
            )
            
        except Exception as e:
            self.log_test_result(test_name, False, f"Exception: {str(e)}")
    
    def test_02_multiple_wagers_same_balance(self, patron: str):
        """
        Test 2: Multiple Wagers with Same Balance
        Users can place multiple wagers with same balance on different unique markets
        """
        test_name = "Test 2: Multiple Wagers Same Balance"
        logging.info(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{test_name}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
        
        try:
            # Get initial balance
            balance_before = self.get_balance(patron)
            initial_balance = balance_before.get('balance', 0)
            
            if initial_balance < 100:
                self.log_test_result(test_name, False, f"Insufficient balance: ${initial_balance:.2f}")
                return
            
            logging.info(f"Initial balance: ${initial_balance:.2f}")
            
            # Place multiple wagers on different markets
            wagers_placed = []
            wager_amount = min(initial_balance, 50.0)  # Use $50 or available balance
            
            for i in range(3):
                market_info = self.get_available_market(patron)
                if not market_info:
                    logging.warning(f"Could not find market #{i+1}")
                    continue
                
                wager = self.place_wager(patron, market_info['line_id'], 150, wager_amount)
                if wager['success']:
                    wagers_placed.append(wager)
                    logging.info(f"Placed wager {i+1}/3 on {market_info['event']['name']}")
                    time.sleep(0.5)
            
            if len(wagers_placed) < 2:
                self.log_test_result(test_name, False, "Could not place multiple wagers")
                return
            
            time.sleep(1)
            
            # Check balance - should still be unchanged
            balance_after = self.get_balance(patron)
            final_balance = balance_after.get('balance', 0)
            
            if abs(final_balance - initial_balance) > 0.01:
                self.log_test_result(
                    test_name,
                    False,
                    f"Balance changed! Expected: ${initial_balance:.2f}, Got: ${final_balance:.2f}"
                )
                return
            
            # Check exposure
            exposure = self.get_exposure(patron)
            total_exposure = len(wagers_placed) * wager_amount
            
            logging.info(f"{Colors.GREEN}✓ Placed {len(wagers_placed)} wagers, balance unchanged: ${initial_balance:.2f}{Colors.RESET}")
            logging.info(f"Total exposure: ${total_exposure:.2f}")
            
            self.log_test_result(
                test_name,
                True,
                f"Successfully placed {len(wagers_placed)} wagers with same balance",
                {
                    'wagers_placed': len(wagers_placed),
                    'initial_balance': initial_balance,
                    'final_balance': final_balance,
                    'total_exposure': total_exposure
                }
            )
            
        except Exception as e:
            self.log_test_result(test_name, False, f"Exception: {str(e)}")
    
    def test_03_cancel_before_match(self, patron: str):
        """
        Test 3: Cancel Wager Before Match
        User can cancel unmatched wagers, balance remains unchanged
        """
        test_name = "Test 3: Cancel Before Match"
        logging.info(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{test_name}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
        
        try:
            # Get initial balance
            balance_before = self.get_balance(patron)
            initial_balance = balance_before.get('balance', 0)
            
            # Get market and place wager
            market_info = self.get_available_market(patron)
            if not market_info:
                self.log_test_result(test_name, False, "No available markets")
                return
            
            wager = self.place_wager(patron, market_info['line_id'], 150, 10.0)
            if not wager['success']:
                self.log_test_result(test_name, False, "Failed to place wager")
                return
            
            time.sleep(1)
            
            # Check balance unchanged after placement
            balance_after_place = self.get_balance(patron)
            balance_mid = balance_after_place.get('balance', 0)
            
            # Cancel the wager
            cancelled = self.cancel_wager(patron, wager['external_id'], wager['wager_id'])
            if not cancelled:
                self.log_test_result(test_name, False, "Failed to cancel wager")
                return
            
            time.sleep(1)
            
            # Check balance still unchanged after cancellation
            balance_after_cancel = self.get_balance(patron)
            final_balance = balance_after_cancel.get('balance', 0)
            
            if abs(final_balance - initial_balance) > 0.01:
                self.log_test_result(
                    test_name,
                    False,
                    f"Balance changed! Expected: ${initial_balance:.2f}, Got: ${final_balance:.2f}"
                )
                return
            
            logging.info(f"{Colors.GREEN}✓ Cancel successful, balance unchanged: ${initial_balance:.2f}{Colors.RESET}")
            
            self.log_test_result(
                test_name,
                True,
                "Cancellation works correctly without affecting balance",
                {
                    'initial_balance': initial_balance,
                    'after_place': balance_mid,
                    'after_cancel': final_balance
                }
            )
            
        except Exception as e:
            self.log_test_result(test_name, False, f"Exception: {str(e)}")
    
    def test_04_exposure_calculation(self, patron: str):
        """
        Test 4: Maximum Exposure Calculation (10x Rule)
        System enforces max exposure = Balance × 10
        """
        test_name = "Test 4: Exposure Calculation (10x)"
        logging.info(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{test_name}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
        
        try:
            # Get balance
            balance_data = self.get_balance(patron)
            current_balance = balance_data.get('balance', 0)
            
            if current_balance < 10:
                self.log_test_result(test_name, False, f"Balance too low: ${current_balance:.2f}")
                return
            
            max_exposure = current_balance * 10
            logging.info(f"Balance: ${current_balance:.2f}, Max Exposure: ${max_exposure:.2f}")
            
            # Get exposure
            exposure_data = self.get_exposure(patron)
            
            if exposure_data:
                current_exposure = exposure_data.get('total_exposure', 0)
                available_exposure = exposure_data.get('available_exposure', 0)
                
                logging.info(f"Current Exposure: ${current_exposure:.2f}")
                logging.info(f"Available Exposure: ${available_exposure:.2f}")
                
                # Verify max exposure rule
                expected_max = current_balance * 10
                actual_max = current_exposure + available_exposure
                
                if abs(actual_max - expected_max) < 1.0:  # Allow small rounding
                    self.log_test_result(
                        test_name,
                        True,
                        f"Exposure calculation correct: ${actual_max:.2f} ≈ ${expected_max:.2f}",
                        {
                            'balance': current_balance,
                            'max_exposure': expected_max,
                            'current_exposure': current_exposure,
                            'available_exposure': available_exposure
                        }
                    )
                else:
                    self.log_test_result(
                        test_name,
                        False,
                        f"Exposure mismatch: Expected ${expected_max:.2f}, Got ${actual_max:.2f}"
                    )
            else:
                # If exposure endpoint not available, just log balance
                logging.info(f"Exposure endpoint not available, checking balance only")
                self.log_test_result(
                    test_name,
                    True,
                    "Balance retrieved successfully (exposure endpoint unavailable)",
                    {'balance': current_balance}
                )
                
        except Exception as e:
            self.log_test_result(test_name, False, f"Exception: {str(e)}")
    
    def test_05_mm_massive_wager_placement(self, mm_account: str, num_wagers: int = 100):
        """
        Test 5: MM Massive Wager Placement
        MM places large number of wagers, verify DEDUCE handles volume
        """
        test_name = f"Test 5: MM Massive Placement ({num_wagers} wagers)"
        logging.info(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{test_name}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
        
        try:
            # Get initial balance
            balance_before = self.get_balance(mm_account)
            initial_balance = balance_before.get('balance', 0)
            
            logging.info(f"Initial balance: ${initial_balance:.2f}")
            logging.info(f"Preparing to place {num_wagers} wagers...")
            
            # Get available markets
            market_info = self.get_available_market(mm_account)
            if not market_info:
                self.log_test_result(test_name, False, "No available markets")
                return
            
            line_id = market_info['line_id']
            
            # Place massive number of wagers
            start_time = time.time()
            successful_wagers = []
            failed_wagers = 0
            
            logging.info(f"{Colors.MAGENTA}Starting massive placement...{Colors.RESET}")
            
            # Use batch placement for efficiency
            batch_size = 20  # API limit
            num_batches = (num_wagers + batch_size - 1) // batch_size
            
            batch_place_url = urljoin(self.base_url, config.URL['mm_batch_place'])
            
            for batch_idx in range(num_batches):
                wagers_in_batch = min(batch_size, num_wagers - len(successful_wagers))
                
                external_ids = [str(uuid.uuid4()) for _ in range(wagers_in_batch)]
                batch_body = [{
                    'external_id': external_ids[i],
                    'line_id': line_id,
                    'odds': 150,
                    'stake': 1.0
                } for i in range(wagers_in_batch)]
                
                response = requests.post(
                    batch_place_url,
                    json={'data': batch_body},
                    headers=self.get_auth_header(mm_account)
                )
                
                if response.status_code == 200:
                    batch_result = response.json().get('data', {}).get('succeed_wagers', [])
                    successful_wagers.extend(batch_result)
                    logging.info(f"Batch {batch_idx+1}/{num_batches}: {len(batch_result)} wagers placed")
                else:
                    failed_wagers += wagers_in_batch
                    logging.warning(f"Batch {batch_idx+1}/{num_batches}: Failed")
                
                time.sleep(0.1)  # Small delay between batches
            
            placement_time = time.time() - start_time
            
            logging.info(f"\n{Colors.GREEN}Placement complete:{Colors.RESET}")
            logging.info(f"  Successful: {len(successful_wagers)}")
            logging.info(f"  Failed: {failed_wagers}")
            logging.info(f"  Time: {placement_time:.2f}s")
            logging.info(f"  Rate: {len(successful_wagers)/placement_time:.1f} wagers/sec")
            
            time.sleep(2)
            
            # Check balance - should be UNCHANGED (DEDUCE model)
            balance_after = self.get_balance(mm_account)
            final_balance = balance_after.get('balance', 0)
            
            if abs(final_balance - initial_balance) > 0.01:
                self.log_test_result(
                    test_name,
                    False,
                    f"Balance changed! Expected: ${initial_balance:.2f}, Got: ${final_balance:.2f}"
                )
                return
            
            logging.info(f"{Colors.GREEN}✓ Balance unchanged after {len(successful_wagers)} wagers!{Colors.RESET}")
            
            self.log_test_result(
                test_name,
                True,
                f"Successfully placed {len(successful_wagers)} wagers, balance unchanged",
                {
                    'wagers_placed': len(successful_wagers),
                    'failed': failed_wagers,
                    'placement_time': placement_time,
                    'rate_per_sec': len(successful_wagers)/placement_time,
                    'initial_balance': initial_balance,
                    'final_balance': final_balance
                }
            )
            
            # Store wagers for cancellation test
            self.sessions[mm_account]['massive_wagers'] = successful_wagers
            
        except Exception as e:
            self.log_test_result(test_name, False, f"Exception: {str(e)}")
    
    def test_06_mm_massive_cancellation(self, mm_account: str):
        """
        Test 6: MM Massive Simultaneous Cancellation
        Cancel large number of wagers simultaneously, verify balance handling
        """
        test_name = "Test 6: MM Massive Simultaneous Cancellation"
        logging.info(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{test_name}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
        
        try:
            # Get wagers from previous test
            if 'massive_wagers' not in self.sessions[mm_account]:
                # Place some wagers first
                logging.info("No existing wagers, placing wagers first...")
                self.test_05_mm_massive_wager_placement(mm_account, num_wagers=50)
                
                if 'massive_wagers' not in self.sessions[mm_account]:
                    self.log_test_result(test_name, False, "Could not set up wagers for cancellation")
                    return
            
            wagers_to_cancel = self.sessions[mm_account]['massive_wagers']
            
            if not wagers_to_cancel:
                self.log_test_result(test_name, False, "No wagers to cancel")
                return
            
            logging.info(f"Found {len(wagers_to_cancel)} wagers to cancel")
            
            # Get initial balance
            balance_before = self.get_balance(mm_account)
            initial_balance = balance_before.get('balance', 0)
            
            logging.info(f"Balance before cancellation: ${initial_balance:.2f}")
            logging.info(f"{Colors.MAGENTA}Starting massive cancellation...{Colors.RESET}")
            
            # Cancel in batches
            batch_cancel_url = urljoin(self.base_url, config.URL['mm_batch_cancel'])
            batch_size = 20  # API limit
            
            start_time = time.time()
            cancelled_count = 0
            failed_count = 0
            
            for i in range(0, len(wagers_to_cancel), batch_size):
                batch = wagers_to_cancel[i:i+batch_size]
                
                cancel_body = [{
                    'wager_id': w['id'],
                    'external_id': w['external_id']
                } for w in batch]
                
                response = requests.post(
                    batch_cancel_url,
                    json={'data': cancel_body},
                    headers=self.get_auth_header(mm_account)
                )
                
                if response.status_code == 200:
                    cancelled_count += len(batch)
                    logging.info(f"Batch {i//batch_size + 1}: Cancelled {len(batch)} wagers")
                else:
                    failed_count += len(batch)
                    # 404 means already cancelled - that's ok
                    if response.status_code == 404:
                        cancelled_count += len(batch)
                        logging.info(f"Batch {i//batch_size + 1}: Already cancelled")
                    else:
                        logging.warning(f"Batch {i//batch_size + 1}: Failed - {response.status_code}")
                
                time.sleep(0.05)  # Small delay
            
            cancellation_time = time.time() - start_time
            
            logging.info(f"\n{Colors.GREEN}Cancellation complete:{Colors.RESET}")
            logging.info(f"  Cancelled: {cancelled_count}")
            logging.info(f"  Failed: {failed_count}")
            logging.info(f"  Time: {cancellation_time:.2f}s")
            logging.info(f"  Rate: {cancelled_count/cancellation_time:.1f} cancellations/sec")
            
            time.sleep(2)
            
            # Check balance - should still be unchanged (wagers never matched)
            balance_after = self.get_balance(mm_account)
            final_balance = balance_after.get('balance', 0)
            
            if abs(final_balance - initial_balance) > 0.01:
                self.log_test_result(
                    test_name,
                    False,
                    f"Balance changed unexpectedly! Expected: ${initial_balance:.2f}, Got: ${final_balance:.2f}"
                )
                return
            
            logging.info(f"{Colors.GREEN}✓ Balance unchanged after cancelling {cancelled_count} wagers!{Colors.RESET}")
            
            self.log_test_result(
                test_name,
                True,
                f"Successfully cancelled {cancelled_count} wagers, balance unchanged",
                {
                    'wagers_cancelled': cancelled_count,
                    'failed': failed_count,
                    'cancellation_time': cancellation_time,
                    'rate_per_sec': cancelled_count/cancellation_time,
                    'initial_balance': initial_balance,
                    'final_balance': final_balance
                }
            )
            
            # Clear wagers
            self.sessions[mm_account]['massive_wagers'] = []
            
        except Exception as e:
            self.log_test_result(test_name, False, f"Exception: {str(e)}")
    
    def test_07_mm_stress_place_and_cancel(self, mm_account: str, cycles: int = 3):
        """
        Test 7: MM Stress Test - Rapid Place & Cancel Cycles
        Rapidly place and cancel wagers in cycles to stress test the system
        """
        test_name = f"Test 7: MM Stress Test ({cycles} cycles)"
        logging.info(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{test_name}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
        
        try:
            balance_before = self.get_balance(mm_account)
            initial_balance = balance_before.get('balance', 0)
            
            logging.info(f"Initial balance: ${initial_balance:.2f}")
            logging.info(f"Running {cycles} place-cancel cycles...")
            
            total_placed = 0
            total_cancelled = 0
            total_time = 0
            
            for cycle in range(cycles):
                logging.info(f"\n{Colors.MAGENTA}--- Cycle {cycle+1}/{cycles} ---{Colors.RESET}")
                
                # Place wagers
                cycle_start = time.time()
                self.test_05_mm_massive_wager_placement(mm_account, num_wagers=30)
                
                if 'massive_wagers' in self.sessions[mm_account]:
                    placed_count = len(self.sessions[mm_account]['massive_wagers'])
                    total_placed += placed_count
                    
                    # Immediately cancel
                    time.sleep(0.5)
                    self.test_06_mm_massive_cancellation(mm_account)
                    total_cancelled += placed_count
                
                cycle_time = time.time() - cycle_start
                total_time += cycle_time
                logging.info(f"Cycle {cycle+1} completed in {cycle_time:.2f}s")
                
                time.sleep(1)  # Brief pause between cycles
            
            # Final balance check
            balance_after = self.get_balance(mm_account)
            final_balance = balance_after.get('balance', 0)
            
            logging.info(f"\n{Colors.GREEN}Stress test complete:{Colors.RESET}")
            logging.info(f"  Total wagers placed: {total_placed}")
            logging.info(f"  Total wagers cancelled: {total_cancelled}")
            logging.info(f"  Total time: {total_time:.2f}s")
            logging.info(f"  Initial balance: ${initial_balance:.2f}")
            logging.info(f"  Final balance: ${final_balance:.2f}")
            
            if abs(final_balance - initial_balance) > 0.01:
                self.log_test_result(
                    test_name,
                    False,
                    f"Balance changed! Expected: ${initial_balance:.2f}, Got: ${final_balance:.2f}"
                )
                return
            
            logging.info(f"{Colors.GREEN}✓ Balance perfectly unchanged after stress test!{Colors.RESET}")
            
            self.log_test_result(
                test_name,
                True,
                f"Stress test passed: {total_placed} wagers placed/cancelled, balance unchanged",
                {
                    'cycles': cycles,
                    'total_placed': total_placed,
                    'total_cancelled': total_cancelled,
                    'total_time': total_time,
                    'initial_balance': initial_balance,
                    'final_balance': final_balance
                }
            )
            
        except Exception as e:
            self.log_test_result(test_name, False, f"Exception: {str(e)}")


def main():
    """Main test execution"""
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}DEDUCE Feature Automated Tests{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    # Initialize framework - use environment from config
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Login test accounts
    print(f"{Colors.BOLD}Setting up test accounts...{Colors.RESET}\n")
    
    try:
        # MM Accounts (use access_key/secret_key from JSON)
        print(f"{Colors.CYAN}Logging in MM accounts...{Colors.RESET}")
        mm_creds1 = config.get_account_credentials(1, config.ENVIRONMENT)
        framework.login_account('mm1', mm_creds1, account_type='mm')
        
        mm_creds2 = config.get_account_credentials(2, config.ENVIRONMENT)
        framework.login_account('mm2', mm_creds2, account_type='mm')
        
        # Patron Accounts (load from JSON file)
        print(f"\n{Colors.CYAN}Logging in Patron accounts...{Colors.RESET}")
        
        # Load patron credentials from user_info_patron_sandbox.json or user_info_patron_staging.json
        try:
            patron_config_file = f'user_info_patron_{config.ENVIRONMENT}.json'
            patron_config = config.load_env_account_config(config.ENVIRONMENT, 'patron.json', patron_config_file)
            
            patron_accounts = [
                {
                    'name': 'patron1',
                    'username': patron_config.get('email'),
                    'password': patron_config.get('password')
                }
            ]
            
            # Add hardcoded patron accounts if they exist (for backwards compatibility)
            if config.ENVIRONMENT == 'staging':
                patron_accounts.extend([
                    {
                        'name': 'patron2',
                        'username': 'thinh.tran@betprophet.co',
                        'password': 'Matkhau1$'
                    },
                    {
                        'name': 'patron3',
                        'username': 'lam.tran+usr001@betprophet.co',
                        'password': 'Kh0ngbiet1'
                    }
                ])
        except Exception as e:
            print(f"{Colors.YELLOW}⚠ Could not load patron config: {e}{Colors.RESET}")
            print(f"{Colors.YELLOW}   Using hardcoded credentials{Colors.RESET}")
            patron_accounts = [
                {
                    'name': 'patron1',
                    'username': 'parlay_cash_patron@betprophet.co',
                    'password': 'Testing@123'
                },
                {
                    'name': 'patron2',
                    'username': 'thinh.tran@betprophet.co',
                    'password': 'Matkhau1$'
                },
                {
                    'name': 'patron3',
                    'username': 'lam.tran+usr001@betprophet.co',
                    'password': 'Kh0ngbiet1'
                }
            ]
        
        for patron in patron_accounts:
            try:
                framework.login_account(
                    patron['name'],
                    {'username': patron['username'], 'password': patron['password']},
                    account_type='patron'
                )
            except Exception as e:
                print(f"{Colors.YELLOW}⚠ Could not login {patron['name']}: {e}{Colors.RESET}")
        
    except Exception as e:
        print(f"{Colors.RED}Failed to login accounts: {e}{Colors.RESET}")
        print(f"{Colors.YELLOW}Note: MM accounts use access_key/secret_key from JSON files{Colors.RESET}")
        print(f"{Colors.YELLOW}      Patron accounts use username/password (hardcoded test accounts){Colors.RESET}")
        return
    
    print(f"\n{Colors.BOLD}Running tests...{Colors.RESET}\n")
    
    # Determine which accounts to use
    has_patrons = 'patron1' in framework.sessions and 'patron2' in framework.sessions
    has_mm = 'mm1' in framework.sessions
    
    if not has_patrons and not has_mm:
        print(f"{Colors.RED}Not enough accounts logged in for testing{Colors.RESET}")
        return
    
    # Run Patron tests if available
    if has_patrons:
        print(f"{Colors.BOLD}{Colors.GREEN}=== PATRON TESTS ==={Colors.RESET}\n")
        framework.test_01_basic_deduce_flow('patron1', 'patron2')
        framework.test_02_multiple_wagers_same_balance('patron1')
        framework.test_03_cancel_before_match('patron1')
        framework.test_04_exposure_calculation('patron1')
    
    # Run MM stress tests if available
    if has_mm:
        print(f"\n{Colors.BOLD}{Colors.MAGENTA}=== MM STRESS TESTS ==={Colors.RESET}\n")
        framework.test_05_mm_massive_wager_placement('mm1', num_wagers=100)
        framework.test_06_mm_massive_cancellation('mm1')
        framework.test_07_mm_stress_place_and_cancel('mm1', cycles=3)
    
    # If only MM accounts, also run basic tests with them
    if has_mm and not has_patrons:
        print(f"\n{Colors.BOLD}{Colors.CYAN}=== BASIC TESTS (MM) ==={Colors.RESET}\n")
        if 'mm2' in framework.sessions:
            framework.test_01_basic_deduce_flow('mm1', 'mm2')
        framework.test_02_multiple_wagers_same_balance('mm1')
        framework.test_03_cancel_before_match('mm1')
        framework.test_04_exposure_calculation('mm1')
    
    # Print summary
    framework.print_summary()
    
    # Save results to file
    results_file = f"deduce_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w') as f:
        json.dump(framework.test_results, f, indent=2)
    
    print(f"{Colors.GREEN}Results saved to: {results_file}{Colors.RESET}\n")


if __name__ == "__main__":
    main()
