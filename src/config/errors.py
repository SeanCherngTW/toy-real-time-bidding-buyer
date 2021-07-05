import sys
import traceback
import psycopg2


class NoAdException(Exception):
    pass


class UndefinedTableError(psycopg2.errors.UndefinedTable):
    pass


class DuplicateTableError(psycopg2.errors.DuplicateTable):
    pass


def error_message(e):
    error_class = e.__class__.__name__
    if len(e.args) == 0:
        return {'status': 500, 'msg': e}
    detail = e.args[0]
    cl, exc, tb = sys.exc_info()
    last_call_stack = traceback.extract_tb(tb)[-1]
    file_name = last_call_stack[0]
    line_num = last_call_stack[1]
    func_name = last_call_stack[2]
    err_msg = "Exception raise in file: {}, line {}, in {}(): [{}] {}.".format(
        file_name, line_num, func_name, error_class, detail
    )
    return {'status': 500, 'msg': err_msg}


def no_content(e):
    return ('', 204)
