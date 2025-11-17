import json
import datetime
import lambda_function
from unittest.mock import patch, MagicMock

@patch("lambda_function.s3")
@patch("lambda_function.urllib3.PoolManager")
def test_lambda_handler(mock_pool_manager, mock_s3):
    # Mock API response
    mock_http = MagicMock()
    mock_http.request.return_value.data = json.dumps({
        "main": {
            "temp": 14.25,
            "feels_like": 13.80,
            "pressure": 1021,
            "humidity": 67
        },
        "weather": [{"main": "Clear", "description": "clear sky"}],
        "wind": {"speed": 3.09},
        "name": "Minneapolis",
        "dt": 1710000000
    }).encode("utf-8")

    mock_pool_manager.return_value = mock_http

    # Mock S3 upload
    mock_s3.put_object.return_value = True

    # Run function
    result = lambda_function.lambda_handler({}, {})

    # Assertions
    assert result["statusCode"] == 200
    assert "Weather data for Minneapolis saved to" in result["body"]
    mock_s3.put_object.assert_called_once()