import time

import requests
import json
import pysher
import base64
import schedule
import random
import threading
import uuid

from urllib.parse import urljoin
# from src import config_staging as config
from src import config
from src.log import logging
from src import constants

GLOCAL_RESULT = []
RUNNING = False
MAX_LATENCY = 0
def _request_post_star(argdict: dict):
    GLOCAL_RESULT.append(requests.post(**argdict))

class MMInteractions:
    base_url: str = None
    balance: float = 0
    mm_keys: dict = dict()
    mm_session: dict = dict()
    all_tournaments: dict = dict()    # mapping from string to id
    my_tournaments: dict = dict()
    sport_events: dict = dict()   # key is event id, value is a list of event details and markets
    wagers: dict = dict()    # all wagers bet in the session
    valid_odds: list = []
    pusher = None

    def __init__(self):
        self.base_url = config.BASE_URL
        self.mm_keys = config.MM_KEYS

    def mm_login(self) -> dict:
        login_url = urljoin(self.base_url, config.URL['mm_login'])
        request_body = {
            'access_key': self.mm_keys.get('access_key'),
            'secret_key': self.mm_keys.get('secret_key'),
        }
        response = requests.post(login_url, data=json.dumps(request_body))
        if response.status_code != 200:
            logging.debug(response)
            raise Exception("login failed")
        mm_session = json.loads(response.content)['data']
        logging.info(mm_session)
        self.mm_session = mm_session
        logging.info("MM session started")
        return mm_session

    def seeding(self):
        # get allowed odds
        logging.info("start to get allowed odds")
        odds_ladder_url = urljoin(self.base_url, config.URL['mm_odds_ladder'])
        odds_response = requests.get(odds_ladder_url, headers=self.__get_auth_header())
        if odds_response.status_code != 200:
            logging.info("not able to get valid odds from api, fall back to local constants")
            self.valid_odds = constants.VALID_ODDS_BACKUP
        else:
            self.valid_odds = odds_response.json()['data']

        # initiate available tournaments/sport_events
        # tournaments
        logging.info("start seeding tournaments/events/markets")
        t_url = urljoin(self.base_url, config.URL['mm_tournaments'])
        headers = self.__get_auth_header()
        all_tournaments_response = requests.get(t_url, headers=headers)
        if all_tournaments_response.status_code != 200:
            raise Exception("not able to seed tournaments")
        all_tournaments = json.loads(all_tournaments_response.content).get('data', {}).get('tournaments', {})
        self.all_tournaments = all_tournaments

        # get sportevents and markets of each
        event_url = urljoin(self.base_url, config.URL['mm_events'])
        market_url = urljoin(self.base_url, config.URL['mm_markets'])
        for one_t in all_tournaments:
            if one_t['name'] in config.TOURNAMENTS_INTERESTED:
                self.my_tournaments[one_t['id']] = one_t
                events_response = requests.get(event_url, params={'tournament_id': one_t['id']}, headers=headers)
                if events_response.status_code == 200:
                    events = json.loads(events_response.content).get('data', {}).get('sport_events')
                    if events is None:
                        continue
                    for event in events:
                        market_response = requests.get(market_url, params={'event_id': event['event_id']},
                                                       headers=headers)
                        if market_response.status_code == 200:
                            markets = json.loads(market_response.content).get('data', {}).get('markets', {})
                            if markets is None:
                                # this is more like a bug in MM api, as the event actually already closed
                                continue
                            event['markets'] = markets
                            self.sport_events[event['event_id']] = event
                            logging.info(f'successfully get markets of events {event["name"]}')
                        else:
                            logging.info(f'failed to get markets of events {event["name"]},'
                                         f' error: {market_response.reason}')
                else:
                    logging.info(f'skip tournament {one_t["name"]} as api request failed')

        logging.info("Done, seeding")
        logging.info(f"found {len(self.my_tournaments)} tournament, ingested {len(self.sport_events)} "
                     f"sport events from {len(config.TOURNAMENTS_INTERESTED)} tournaments")
        for key in self.sport_events:
            one_event = self.sport_events[key]
            for market in one_event['markets']:
                if 'selections' in market:
                    if len(market['selections']) == 0:
                        raise Exception(f"selection is empty for event {key}")
                    for selection in market['selections']:
                        if selection[0].get('line_id', None) is None:
                            pass
                            #raise Exception(f'line_id is empty for event {key}')
                        print(selection[0]['line_id'])
                elif 'market_lines' in market:
                    for market_line in market['market_lines']:
                        if 'selections' not in market_line or len(market_line['selections']) == 0:
                            raise Exception(f'selections is empty')
                        for selection in market_line['selections']:
                            if selection[0].get('line_id', None) is None:
                                raise Exception(f'line_id is empty for event {key}')
                            print(selection[0]['line_id'])
                else:
                    #raise Exception("no selection, no market_lines")
                    print("no selection, no market_lines")
        print("validated")


    def _get_channels(self, socket_id: float):
        # get websocket channels to subscribe to
        auth_endpoint_url = urljoin(self.base_url, config.URL['mm_auth'])
        # auth_endpoint_url = "http://localhost:19002/api/v1/mm/pusher"
        channels_response = requests.post(auth_endpoint_url,
                                          data={'socket_id': socket_id},
                                          headers=self.__get_auth_header())
        if channels_response.status_code != 200:
            logging.error("failed to get channels")
            raise Exception("failed to get channels")
        channels = channels_response.json()
        return channels.get('data', {}).get('authorized_channel', [])
    def subscribe(self):
        auth_endpoint_url = urljoin(self.base_url, config.URL['mm_auth'])
        auth_header = self.__get_auth_header()
        auth_headers = {
                           "Authorization": auth_header['Authorization'],
                           "header-subscriptions": '''[{"type":"tournament","ids":[]}]''',
                       }
        self.pusher = pysher.Pusher(key=config.MM_APP_KEY, cluster=config.APP_CLUSTER,
                                    auth_endpoint=auth_endpoint_url,
                                    auth_endpoint_headers=auth_headers)

        def public_event_handler(*args, **kwargs):
            print("processing public, Args:", args)
            print(f"event details {base64.b64decode(json.loads(args[0]).get('payload', '{}'))}")
            print("processing public, Kwargs:", kwargs)

        def private_event_handler(*args, **kwargs):
            global MAX_LATENCY
            print("processing private, Args:", args)
            arg_dict = json.loads(args[0])
            print(f"msg sent out at: {arg_dict.get('timestamp', 0) / 1000} \n event details {base64.b64decode(arg_dict.get('payload', '{}'))}")
            payload = json.loads(base64.b64decode(arg_dict.get('payload', '{}')))
            if arg_dict.get('change_type') == 'wagers':
                if 'sequence_number' in payload['info'] and payload['info']['update_type'] == 'status':
                    latency = (arg_dict.get('timestamp', 0)/1000 - payload['info']['sequence_number'])/1000
                    print(f"timestamp - sequence number : {latency} ms")
                    if latency > MAX_LATENCY:
                        MAX_LATENCY = latency
            x = 1 + 1
            '''if len(payload) > 0:
                if 'info' in payload and 'status' in payload['info']:
                    if payload['info']['status'] == 'open':
                        external_id = payload['info']['external_id']
                        wager_id = payload['info']['id']
                        body = {
                            'external_id': external_id,
                            'wager_id': wager_id,
                        }
                        cancel_url = urljoin(self.base_url, config.URL['mm_cancel_wager'])
                        cancel_response = threading.Thread(target=_request_post_star,
                                                           args=({'url': cancel_url, 'json': body,
                                                                  'headers': self.__get_auth_header()},))
                        cancel_response1_1 = threading.Thread(target=_request_post_star,
                                                           args=({'url': cancel_url, 'json': body,
                                                                  'headers': self.__get_auth_header()},))
                        cancel_response2 = threading.Thread(target=_request_post_star,
                                                            args=({'url': cancel_url, 'json': body,
                                                                   'headers': self.__get_auth_header()},))
                        cancel_response2_2 = threading.Thread(target=_request_post_star,
                                                            args=({'url': cancel_url, 'json': body,
                                                                   'headers': self.__get_auth_header()},))
                        cancel_response.start()
                        cancel_response1_1.start()
                        cancel_response2.start()
                        cancel_response2_2.start()
                        print(GLOCAL_RESULT)
                print("here")'''

            print("processing private, Kwargs:", kwargs)

        # We can't subscribe until we've connected, so we use a callback handler
        # to subscribe when able
        def connect_handler(data):
            socket_id = json.loads(data)['socket_id']
            available_channels = self._get_channels(socket_id)
            broadcast_channel_name = None
            private_channel_name = None
            private_events = None
            for channel in available_channels:
                if 'broadcast' in channel['channel_name']:
                    broadcast_channel_name = channel['channel_name']
                else:
                    private_channel_name = channel['channel_name']
                    private_events = channel['binding_events']
            broadcast_channel = self.pusher.subscribe(broadcast_channel_name)
            private_channel = self.pusher.subscribe(private_channel_name)
            for t_id in self.my_tournaments:
                event_name = f'tournament_{t_id}'
                broadcast_channel.bind(event_name, public_event_handler)
                logging.info(f"subscribed to public channel, event name: {event_name}, successfully")

            for private_event in private_events:
                private_channel.bind(private_event['name'], private_event_handler)
                logging.info(f"subscribed to private channel, event name: {private_event['name']}, successfully")

        self.pusher.connection.bind('pusher:connection_established', connect_handler)
        self.pusher.connect()

    def get_balance(self):
        balance_url = urljoin(self.base_url, config.URL['mm_balance'])
        response = requests.get(balance_url, headers=self.__get_auth_header())
        if response.status_code != 200:
            logging.error("failed to get balance")
            return
        self.balance = json.loads(response.content).get('data', {}).get('balance', 0)
        logging.info(f"still have ${self.balance} left")

    def start_betting(self):
        global RUNNING
        if RUNNING:
            return
        logging.info("Start betting, randomly :)")
        bet_url = urljoin(self.base_url, config.URL['mm_place_wager'])
        batch_bet_url = urljoin(self.base_url, config.URL['mm_batch_place'])
        if '.prophetbettingexchange' in bet_url:
            raise Exception("only allowed to run in non production environment")
        for key in self.sport_events:
            one_event = self.sport_events[key]
            for market in one_event.get('markets', []):
                if True: #market['type'] == 'moneyline':
                    # only bet on moneyline
                    selections = market.get('selections', [])
                    if random.random() < 0.2:   # 30% chance to bet
                        if 'market_lines' in market:
                            favorite_lines = [x.get('selections', []) for x in market['market_lines'] if x.get('favourite', False)]
                            if len(favorite_lines) < 1:
                                continue
                            selections = favorite_lines[0]
                        if len(selections) == 0:
                            error_code = f"selections should not be empty for event {one_event['event_id']}"
                            logging.error(error_code)
                            #raise Exception(error_code)
                            continue
                        for selection in selections:
                            if random.random() < 0.2: #30% chance to bet
                                RUNNING = True
                                picked_selection = 0
                                odds_to_bet = self.__get_random_odds()
                                external_id = str(uuid.uuid1())
                                logging.info(f"going to bet on '{one_event['name']}' on {market['type']}, side {selection[picked_selection]['name']} with odds {odds_to_bet}")
                                body_to_send = {
                                    'external_id': external_id,
                                    'line_id': selection[picked_selection]['line_id'],
                                    'odds': odds_to_bet,
                                    'stake': 1.0
                                }
                                try:
                                    bet_response = requests.post(bet_url, json=body_to_send,
                                                                 headers=self.__get_auth_header())
                                    # additional concurrently bets
                                    concurrent_n = 31
                                    for j in range(5):
                                        concurrent_requests = []
                                        for i in range(concurrent_n):
                                            external_id = str(uuid.uuid1())
                                            body_to_send_tmp = {
                                                'external_id': external_id,
                                                'line_id': selection[picked_selection]['line_id'],
                                                'odds': odds_to_bet,
                                                'stake': 1.0
                                            }
                                            concurrent_requests.append(threading.Thread(target=_request_post_star,
                                                                                        args=({'url': bet_url, 'json': body_to_send_tmp,
                                                                                               'headers': self.__get_auth_header()},)))
                                        for c_request in concurrent_requests:
                                            c_request.start()
                                        time.sleep(1)

                                    # print(GLOCAL_RESULT)
                                    # external_id = str(uuid.uuid1())
                                    # body_to_send = {
                                    #    'external_id': external_id,
                                    #    'line_id': selection[picked_selection]['line_id'],
                                    #    'odds': odds_to_bet,
                                    #    'stake': 15000.0
                                    # }
                                    # bet_response2 = requests.post(bet_url, json=body_to_send,
                                    #                             headers=self.__get_auth_header())
                                    # print("testing")
                                except Exception as e:
                                    logging.warning(e)
                                    continue
                                if bet_response.status_code != 200:
                                    logging.info(f"failed to bet, error {bet_response.content}")
                                else:
                                    logging.info("successfully")
                                    self.wagers[external_id] = json.loads(bet_response.content).get('data', {})['wager']['id']
                                    # test cancel wager immediately
                                    '''
                                    body = {
                                        'external_id': external_id,
                                        'wager_id': json.loads(bet_response.content).get('data', {})['wager']['id'],
                                    }
                                    cancel_url = urljoin(self.base_url, config.URL['mm_cancel_wager'])
                                    cancel_response = threading.Thread(target=_request_post_star,
                                                                       args=({'url': cancel_url, 'json': body,
                                                                              'headers': self.__get_auth_header()},))
                                    cancel_response2 = threading.Thread(target=_request_post_star,
                                                                        args=({'url': cancel_url, 'json': body,
                                                                              'headers': self.__get_auth_header()},))
                                    cancel_response.start()
                                    cancel_response2.start()
                                    cancel_response.join()
                                    cancel_response2.join()
                                    print(GLOCAL_RESULT)
                                    '''
                                    '''
                                    batch_cancel_body = [{'wager_id': x,
                                                          'external_id': external_id} for x in [json.loads(bet_response.content).get('data', {})['wager']['id']]]
                                    batch_cancel_url = urljoin(self.base_url, config.URL['mm_batch_cancel'])
                                    cancel_response = requests.post(batch_cancel_url, json={'data': batch_cancel_body},
                                                             headers=self.__get_auth_header())
                                    print(cancel_response)
                                    '''
                                # testing batch place wagers
                                batch_n = 2
                                external_id_batch = [str(uuid.uuid1()) for x in range(batch_n)]
                                batch_body_to_send = [{
                                    'external_id': external_id_batch[x],
                                    'line_id': selection[picked_selection]['line_id'],
                                    'odds': odds_to_bet,
                                    'stake': 1.0
                                } for x in range(batch_n)]
                                batch_bet_response = requests.post(batch_bet_url, json={"data": batch_body_to_send},
                                                                   headers=self.__get_auth_header())
                                if batch_bet_response.status_code != 200:
                                    logging.info(f"failed to bet, error {bet_response.content}")
                                else:
                                    logging.info("successfully")
                                    for wager in batch_bet_response.json()['data']['succeed_wagers']:
                                        self.wagers[wager['external_id']] = wager['id']
        RUNNING = False

    def random_cancel_wager(self):
        wager_keys = list(self.wagers.keys())
        for key in wager_keys:
            if key not in self.wagers:
                # just in case already canceled by another thread
                continue
            wager_id = self.wagers[key]
            cancel_url = urljoin(self.base_url, config.URL['mm_cancel_wager'])
            if random.random() < 0.5:  # 50% cancel
                logging.info("start to cancel wager")
                body = {
                    'external_id': key,
                    'wager_id': wager_id,
                }
                response = requests.post(cancel_url, json=body, headers=self.__get_auth_header())
                if response.status_code != 200:
                    if response.status_code == 404:
                        logging.info("already cancelled")
                        if key in self.wagers:
                            self.wagers.pop(key)
                    else:
                        logging.info("failed to cancel")
                else:
                    logging.info("cancelled successfully")
                    self.wagers.pop(key)

    def random_batch_cancel_wagers(self):
        wager_keys = list(self.wagers.keys())
        batch_keys_to_cancel = random.choices(wager_keys, k=min(4, len(wager_keys)))
        batch_cancel_body = [{'wager_id': self.wagers[x],
                              'external_id': x} for x in batch_keys_to_cancel]
        batch_cancel_url = urljoin(self.base_url, config.URL['mm_batch_cancel'])
        response = requests.post(batch_cancel_url, json={'data': batch_cancel_body}, headers=self.__get_auth_header())
        if response.status_code != 200:
            if response.status_code == 404:
                logging.info("already cancelled")
                [self.wagers.pop(x) for x in batch_keys_to_cancel]
            else:
                logging.info("failed to cancel")
        else:
            logging.info("cancelled successfully")
            for key in batch_keys_to_cancel:
                try:
                    self.wagers.pop(key)
                except Exception as e:
                    print(e)

    def cancel_all_wagers(self):
        # TODO: upon urgency, I need to cancel all wagers, how to do it?
        print("cancel all wagers")

    def schedule_in_thread(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def __auto_extend_session(self):
        # need to use new api, for now just create new session to pretend session extended
        refresh_url = urljoin(self.base_url, config.URL['mm_refresh'])
        response = requests.post(refresh_url, json={'refresh_token': self.mm_session['refresh_token']},
                                 headers=self.__get_auth_header())
        if response.status_code != 200:
            logging.info("Failed to call refresh endpoint")
        else:
            self.mm_session['access_token'] = response.json()['data']['access_token']
            if self.pusher is not None:
                self.pusher.disconnect()
                self.pusher = None
            self.subscribe()    # need to subscribe again, as the old access token will expire soon

    def auto_betting(self):
        logging.info("schedule to bet every 10 seconds")
        schedule.every(5).seconds.do(self.start_betting)
        schedule.every(9).seconds.do(self.random_cancel_wager)
        schedule.every(7).seconds.do(self.random_batch_cancel_wagers)
        schedule.every(8).minutes.do(self.__auto_extend_session)

        child_thread = threading.Thread(target=self.schedule_in_thread, daemon=False)
        child_thread.start()

    def keep_alive(self):
        child_thread = threading.Thread(target=self.schedule_in_thread, daemon=False)
        child_thread.start()

    def __get_auth_header(self) -> dict:
        return {
            'Authorization': f'Bearer '
                             f'{self.mm_session["access_token"]}',
        }

    def __get_random_odds(self):
        odds = self.valid_odds[random.randint(0, len(self.valid_odds) - 1)]
        odds = odds if random.random() < 0.5 else -1 * odds
        if odds == -100:
            odds = 100
        return odds



