"""Connection retries, safe error messages and unchanged authentication rules."""
from http.client import IncompleteRead,RemoteDisconnected
import io
import socket
import ssl
import unittest
from unittest.mock import Mock,patch
from urllib.error import HTTPError,URLError

from lineage_app.clients.airflow import AirflowClient,APIError


class ConnectionTests(unittest.TestCase):
    def client(self):
        client=AirflowClient('https://airflow.example.com','private-test-token','uat')
        client.cancel=Mock()
        client.cancel.is_set.return_value=False
        client.cancel.wait.return_value=False
        return client

    def test_temporary_disconnect_retries_and_recovers_with_same_authorization(self):
        client=self.client()
        opener=Mock();opener.open.side_effect=[URLError(ConnectionResetError()),io.BytesIO(b'{"dags":[]}')]
        with patch('lineage_app.clients.airflow.build_opener',return_value=opener):
            self.assertEqual(client.get('/api/v2/dags'),{'dags':[]})
        self.assertEqual(opener.open.call_count,2)
        requests=[call.args[0] for call in opener.open.call_args_list]
        self.assertTrue(all(r.get_header('Authorization')=='Bearer private-test-token' for r in requests))
        self.assertFalse(client.auth_failed)

    def test_permanent_network_failures_get_specific_safe_messages(self):
        cases=[(URLError(socket.gaierror(-2,'private-test-token')),'Cannot resolve'),
               (URLError(TimeoutError('private-test-token')),'timed out'),
               (TimeoutError('private-test-token'),'timed out'),
               (ConnectionRefusedError('private-test-token'),'refused'),
               (RemoteDisconnected('private-test-token'),'interrupted'),
               (IncompleteRead(b'private-test-token',50),'interrupted'),
               (URLError('private-test-token'),'connection failed')]
        for error,message in cases:
            with self.subTest(error=type(error).__name__):
                client=self.client();opener=Mock();opener.open.side_effect=error
                with patch('lineage_app.clients.airflow.build_opener',return_value=opener):
                    with self.assertRaisesRegex(RuntimeError,message) as result:client.get('/api/v2/dags')
                self.assertNotIn('private-test-token',str(result.exception))
                self.assertEqual(opener.open.call_count,3)
                self.assertFalse(client.auth_failed)

    def test_certificate_verification_stays_enabled_and_is_not_retried(self):
        client=self.client();opener=Mock()
        opener.open.side_effect=URLError(ssl.SSLCertVerificationError(1,'private-test-token'))
        with patch('lineage_app.clients.airflow.build_opener',return_value=opener):
            with self.assertRaisesRegex(RuntimeError,'certificate verification failed') as result:client.get('/api/v2/dags')
        self.assertNotIn('private-test-token',str(result.exception))
        self.assertEqual(opener.open.call_count,1)
        client.cancel.wait.assert_not_called()

    def test_cancel_during_retry_stops_immediately(self):
        client=self.client();client.cancel.wait.return_value=True
        opener=Mock();opener.open.side_effect=URLError(TimeoutError())
        with patch('lineage_app.clients.airflow.build_opener',return_value=opener):
            with self.assertRaisesRegex(RuntimeError,'cancelled'):client.get('/api/v2/dags')
        self.assertEqual(opener.open.call_count,1)

    def test_unauthorized_response_is_not_a_network_retry_or_token_leak(self):
        client=self.client();opener=Mock()
        opener.open.side_effect=HTTPError('https://airflow.example.com',401,'Unauthorized',{},None)
        with patch('lineage_app.clients.airflow.build_opener',return_value=opener):
            with self.assertRaises(APIError):client.get('/api/v2/dags')
        self.assertTrue(client.auth_failed)
        self.assertEqual(opener.open.call_count,1)
        client.cancel.wait.assert_not_called()


if __name__=='__main__':unittest.main()
