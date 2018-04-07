import sys
from contextlib import contextmanager

import blpapi
HISTORICAL_DATA_REQUEST = 'HistoricalDataRequest'


class BbgCoreService:
    def __init__(self, server_host, server_port, connect_timeout, auto_restart_on_disconnection, num_start_attempts):
        # Fill SessionOptions
        session_options = blpapi.SessionOptions()
        session_options.setServerHost(server_host)
        session_options.setServerPort(server_port)
        session_options.setConnectTimeout(connect_timeout)
        session_options.setAutoRestartOnDisconnection(auto_restart_on_disconnection)
        session_options.setNumStartAttempts(num_start_attempts)

        print("Connecting to bloomberg host %s:%d" % (server_host, server_port))

        # Create a Session
        self.session = blpapi.Session(session_options)

        # Start a Session
        if not self.session.start():
            print("Failed to start session.")
            raise ConnectionError

        if not self.session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
            raise ConnectionError

        self.ref_data_service = self.session.getService("//blp/refdata")

    def get_new_request(self, request_type):
        return self.ref_data_service.createRequest(request_type)

    @contextmanager
    def get_response_from_bbg(self, request):
        print("Sending Request: %s", request)
        self.session.sendRequest(request)

        # Process received events
        try:
            while True:
                # We provide timeout to give the chance to Ctrl+C handling:
                ev = self.session.nextEvent(500)
                for msg in ev:
                    if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                        yield msg

                # Response completely received, so we could exit
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
        except:
            print("Error: {} | when processing request: {}".format(sys.exc_info()[0], request))
            raise

    def __del__(self):
        # Stop the session
        self.session.stop()
        print('Session stopped.')


class BbgService:
    def __init__(self, server_host='localhost', server_port=8194, connect_timeout=20000,
                 auto_restart_on_disconnection=True, num_start_attempts=10):
        self._bbg_core_service = BbgCoreService(server_host, server_port, connect_timeout,
                                                auto_restart_on_disconnection, num_start_attempts)

    def get_data_historical(self, securities, fields, options, overrides):
        """ return data, field exceptions, response error message of a security.
        :param securities: ['1 HK Equity', '2 HK Equity']
        :param fields: ['PX_OPEN', 'PX_LAST']
        :param options: {'startDate': '20150111',
                         'endDate': '20150112'
                         }
        :param overrides: {'BEST_DATA_SOURCE_OVERRIDE': 'BLI',
                            'ALLOW_DYNAMIC_CASHFLOW_CALCS': 'Y'
                            }
        :returns
        {'1 HK Equity':
            [ {'date': datetime(2015, 1, 11),
                 'PX_OPEN': 10.1,
                 'PX_LAST': 12.1,
              },
              {'date': datetime(2015, 1, 12),
                 'PX_OPEN': 11.2,
                 'PX_LAST': 12.2,
              }
            ],
        '2 HK Equity':
            [ {'date': datetime(2015, 1, 11),
                 'PX_OPEN': 13.1,
                 'PX_LAST': 14.1,
              },
              {'date': datetime(2015, 1, 12),
                 'PX_OPEN': 13.2,
                 'PX_LAST': 14.2,
              }
            ],
        }
        """
        result = {}
        for sec in securities:
            result[sec] = self._get_data_historical_one_security(sec, fields, options, overrides)
        return result

    def _get_data_historical_one_security(self, security_name, fields, options, overrides):
        """ return data, field exceptions, response error message of a security.
        :param security_name: '1 HK Equity'
        :param fields: ['PX_OPEN', 'PX_LAST']
        :param options: {'startDate': '20150111',
                         'endDate': '20150112'
                         }
        :param overrides: {'BEST_DATA_SOURCE_OVERRIDE': 'BLI',
                            'ALLOW_DYNAMIC_CASHFLOW_CALCS': 'Y'
                            }
        :returns
        data: [ {'date': datetime(2015, 1, 11),
                 'PX_OPEN': 10.1,
                 'PX_LAST': 12.1,
                },
                {'date': datetime(2015, 1, 12),
                 'PX_OPEN': 11.2,
                 'PX_LAST': 12.2,
                }
              ]
        """
        request = self._bbg_core_service.get_new_request(HISTORICAL_DATA_REQUEST)
        request.append('securities', security_name)

        for fld in fields:
            request.append('fields', fld)

        for option_name, option_value in options.items():
            try:
                request.set(option_name, option_value)
            except:
                raise RuntimeError(
                    'Error when setting Bloomberg request options. | security={} | option_name={} | option_value={}'
                        .format(security_name, option_name, option_value))

        overrides_element = request.getElement('overrides')
        for override_field, override_value in overrides.items():
            try:
                override_element = overrides_element.appendElement()
                override_element.setElement('fieldId', override_field)
                override_element.setElement('value', override_value)
            except:
                raise RuntimeError(
                    'Error when setting Bloomberg request override. | security={} | override_name={} | override_value={}'
                        .format(security_name, override_field, override_value))

        with self._bbg_core_service.get_response_from_bbg(request) as response:
            return self._process_response_historical(response)

    def _process_response_historical(self, response):
        if response.hasElement('responseError'):
            response_error_msg = response.getElement('responseError').getElement('message').getValue()
            raise RuntimeError('Response error returned by Bloomberg: {}'.format(response_error_msg))
        security_data = response.getElement('securityData')
        security_name = security_data.getElement('security').getValueAsString()
        field_data_element = security_data.getElement('fieldData')
        field_exceptions_element = security_data.getElement('fieldExceptions')

        self._raise_error_if_security_error(security_data)

        data = self._extract_data_historical(field_data_element)

        field_exceptions = self._extract_field_exceptions(field_exceptions_element)
        if len(field_exceptions) > 0:
            raise RuntimeError('Fields Exception: {}'.format(str(field_exceptions)))
        return data

    @staticmethod
    def _raise_error_if_security_error(security_data):
        if security_data.hasElement('securityError'):
            security_name = security_data.getElement('security').getValue()
            error_msg = security_data.getElement('securityError').getElement('message').getValue()
            raise RuntimeError('Security Error returned by Bloomberg'.format(error_msg))

    @staticmethod
    def _extract_field_exceptions(field_exceptions):
        """return in format:
            { 'OPT_UNDL_TICKER':    { 'source': '234::bbdbd5',
                                      'code': 9,
                                      'category': 'BAD_FLD',
                                      'message': 'Field not applicable to security',
                                      'subcategory' = 'NOT_APPLICABLE_TO_REF_DATA'
                                      },
              'FUT_CUR_GEN_TICKER': { 'source': '234::bbdbd5',
                                      'code': 9,
                                      'category': 'BAD_FLD',
                                      'message': 'Field not applicable to security',
                                      'subcategory' = 'NOT_APPLICABLE_TO_REF_DATA'
                                      }
            }
        """
        result = {}
        for j in range(field_exceptions.numValues()):
            field_exception = field_exceptions.getValue(j)
            field_name = field_exception.getElement('fieldId').getValueAsString()  # 'OPT_UNDL_TICKER'
            error_info = field_exception.getElement('errorInfo')
            source = error_info.getElement('source').getValue()
            code = error_info.getElement('code').getValue()
            category = error_info.getElement('category').getValue()
            message = error_info.getElement('message').getValue()
            subcategory = error_info.getElement('subcategory').getValue() if error_info.hasElement('subcategory') else ""

            result[field_name] = {}
            result[field_name]['source'] = source
            result[field_name]['code'] = code
            result[field_name]['category'] = category
            result[field_name]['message'] = message
            result[field_name]['subcategory'] = subcategory

        return result

    @staticmethod
    def _extract_data_historical(field_data_array):
        data_all_dates = []

        for i in range(field_data_array.numValues()):
            field_data = field_data_array.getValueAsElement(i)
            data_one_date = {}

            for j in range(field_data.numElements()):
                field = field_data.getElement(j)
                field_name = str(field.name())
                field_value = field.getValue()

                data_one_date[field_name] = field_value

            data_all_dates.append(data_one_date)
        return data_all_dates
