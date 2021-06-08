RESPONSE_TYPE_KEY = "responseType"
RESPONSE_CODE_KEY = "responseCodes"
STATUS_CODE_KEY = "statusCodes"
STATUS_MESSAGE_KEY = "statusMessage"
DATA_KEY = "data"

SUCCESS_CODE = 0
ERROR_CODE_6001 = 6001
ERROR_CODE_6002 = 6002
ERROR_CODE_6003 = 6003
ERROR_CODE_6004 = 6004

ERROR_FAILED = "FAILED"
INFO_SUCCESS = "SUCCESS"

ERROR_MSG_6001 = "ERROR: input parameter is Null"
ERROR_MSG_6002 = "ERROR: Wrong Input parameters. No data mapped output."
ERROR_MSG_6003 = "ERROR: External API Failed."
ERROR_MSG_6004 = "ERROR: DB Operation Failed."

def return_error_result_6001(data):
    global RESPONSE_TYPE_KEY
    global ERROR_FAILED
    global RESPONSE_CODE_KEY
    global STATUS_CODE_KEY
    global ERROR_CODE_6001
    global STATUS_MESSAGE_KEY
    global ERROR_MSG_6001
    global DATA_KEY
    return {
        RESPONSE_TYPE_KEY: ERROR_FAILED,
        RESPONSE_CODE_KEY: {
            STATUS_CODE_KEY: ERROR_CODE_6001,
            STATUS_MESSAGE_KEY: ERROR_MSG_6001
        },
        DATA_KEY: data
    }

def return_success_result(data):
    global RESPONSE_TYPE_KEY
    global INFO_SUCCESS
    global RESPONSE_CODE_KEY
    global STATUS_CODE_KEY
    global SUCCESS_CODE
    global DATA_KEY
    return {
        RESPONSE_TYPE_KEY: INFO_SUCCESS,
        RESPONSE_CODE_KEY: {
            STATUS_CODE_KEY: SUCCESS_CODE
        },
        DATA_KEY: data
    }

def return_error_result_6002(data):
    global RESPONSE_TYPE_KEY
    global ERROR_FAILED
    global RESPONSE_CODE_KEY
    global STATUS_CODE_KEY
    global ERROR_CODE_6002
    global STATUS_MESSAGE_KEY
    global ERROR_MSG_6002
    global DATA_KEY
    return {
        RESPONSE_TYPE_KEY: ERROR_FAILED,
        RESPONSE_CODE_KEY: {
            STATUS_CODE_KEY: ERROR_CODE_6002,
            STATUS_MESSAGE_KEY: ERROR_MSG_6002
        },
        DATA_KEY: data
    }

def return_error_result_6003(data):
    global RESPONSE_TYPE_KEY
    global INFO_SUCCESS
    global RESPONSE_CODE_KEY
    global STATUS_CODE_KEY
    global ERROR_CODE_6003
    global STATUS_MESSAGE_KEY
    global ERROR_MSG_6003
    global DATA_KEY
    return {
        RESPONSE_TYPE_KEY: INFO_SUCCESS,
        RESPONSE_CODE_KEY: {
            STATUS_CODE_KEY: ERROR_CODE_6003,
            STATUS_MESSAGE_KEY: ERROR_MSG_6003
        },
        DATA_KEY: data
    }

def return_error_result_6004(data, msg="ERROR: DB Operation Failed."):
    global RESPONSE_TYPE_KEY
    global INFO_SUCCESS
    global RESPONSE_CODE_KEY
    global STATUS_CODE_KEY
    global ERROR_CODE_6004
    global STATUS_MESSAGE_KEY
    global ERROR_MSG_6004
    global DATA_KEY
    return {
        RESPONSE_TYPE_KEY: INFO_SUCCESS,
        RESPONSE_CODE_KEY: {
            STATUS_CODE_KEY: ERROR_CODE_6004,
            STATUS_MESSAGE_KEY: ERROR_MSG_6004
        },
        DATA_KEY: data
    }