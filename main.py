#!/usr/bin/python3

import datetime
import sys
import getopt
import time

import pymysql


class Clear:
    def __init__(self, cursor, conn):
        # Connect to the database
        self.cursor = cursor
        self.conn = conn

    def partner_record(self, lt_day):
            querySql = "select * from partner_record where id < %s limit 1;"
            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

            while result is not None:
                clearSql = "delete from partner_record where id < %s limit 500000;"
                cursor.execute(clearSql, lt_day)
                self.conn.commit()

                self.cursor.execute(querySql, lt_day)
                result = self.cursor.fetchone()

    # 返佣流水表
    def partner_flow(self, lt_day):
        querySql = "select * from partner_flow where day < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from partner_flow where day < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def dp_rebate_freeze_log(self, lt_day):
        querySql = "select * from dp_rebate_freeze_log where create_time < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from dp_rebate_freeze_log where create_time < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def dp_report_incurred(self, lt_day):
        querySql = "select * from dp_report_incurred where create_time < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from dp_report_incurred where create_time< %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def super_statistics(self, lt_day):
        querySql = "select * from super_statistics where day < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from super_statistics where day < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def agent_sketch_list(self, lt_day):
        querySql = "select * from agent_sketch_list where day < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from agent_sketch_list where day < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def rs_dwd_user_account_currency_detail_h(self, lt_day):
        querySql = "select * from rs_dwd_user_account_currency_detail_h where date < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from rs_dwd_user_account_currency_detail_h where date < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def dp_edm_send_log(self, lt_day):
        querySql = "select * from dp_edm_send_log where create_time < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from dp_edm_send_log where create_time < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def contract_record(self, lt_day):
        querySql = "select * from contract_record where current_time < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from contract_record where current_time < %s limit 500000;"
            cursor.execute(clearSql, lt_day)

            self.conn.commit()
            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def t_D_Trade(self, lt_day):
        querySql = "select * from t_D_Trade where TradeTime < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from t_D_Trade where TradeTime < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def t_D_AccountDetail(self, lt_day):
        querySql = "select * from t_D_AccountDetail where UpdateTime < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from t_D_AccountDetail where UpdateTime < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def t_D_FinishOrder(self, lt_day):
        querySql = "select * from t_D_FinishOrder where UpdateTime < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from t_D_FinishOrder where UpdateTime < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def t_D_FinishTriggerOrder(self, lt_day):
        querySql = "select * from t_D_FinishTriggerOrder where UpdateTime < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from t_D_FinishTriggerOrder where UpdateTime < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()

    def t_D_FinishPosition(self, lt_day):
        querySql = "select * from t_D_FinishPosition where UpdateTime < %s limit 1;"
        self.cursor.execute(querySql, lt_day)
        result = self.cursor.fetchone()

        while result is not None:
            clearSql = "delete from t_D_FinishPosition where UpdateTime < %s limit 500000;"
            cursor.execute(clearSql, lt_day)
            self.conn.commit()

            self.cursor.execute(querySql, lt_day)
            result = self.cursor.fetchone()


def clear_time_format(current_time, cleaning_days, format):
    if format == "%Y%m%d":
        # format为"%Y%m%d"适配时间格式为20230910这种格式
        past_time = current_time - datetime.timedelta(days=cleaning_days)
        past_time_string = past_time.strftime("%Y-%m-%d")
        past_time_int = int(past_time_string.replace("-", ""))

        return past_time_int


if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "",
                                   ["host=", "user=", "password=", "port=", "database=", "cleaning_method="])
    except getopt.GetoptError:
        print("%s --host <host> --user <user> --password <password> --port <port> --database <database> "
              "--cleaning_method <cleaning_method>" % (sys.argv[0]))
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print("%s --host <host> --user <user> --password <password> --port <port> --database <database> "
                  "--cleaning_method <cleaning_method> --cleaning_days <cleaning_days>" % (sys.argv[0]))
            sys.exit()
        elif opt in "--host":
            host = arg
        elif opt in "--user":
            user = arg
        elif opt in "--password":
            password = arg
        elif opt in "--port":
            port = int(arg)
        elif opt in "--database":
            database = arg
        elif opt in "--cleaning_method":
            cleaning_method = arg

    conn = pymysql.connect(host=host,
                           user=user,
                           password=password,
                           port=port,
                           database=database,
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

    current_time = datetime.datetime.now()
    # clear = Clear(conn)

    with conn:
        with conn.cursor() as cursor:
            clear = Clear(cursor, conn)

            if cleaning_method == "partner_flow":
                # partner_flow表，默认清理60天前的
                cleaning_days = 60

                cleaning_time = clear_time_format(current_time, cleaning_days, "%Y%m%d")
                print(cleaning_time)
                clear.partner_flow(cleaning_time)
            elif cleaning_method == "dp_rebate_freeze_log":
                # dp_rebate_freeze_log表，默认清理60天前的
                cleaning_days = 60

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.dp_rebate_freeze_log(cleaning_time)
            elif cleaning_method == "dp_report_incurred":
                # dp_report_incurred表，默认清理30天前的
                cleaning_days = 30

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.dp_report_incurred(cleaning_time)
            elif cleaning_method == "super_statistics":
                # super_statistics表，默认清理180天前的
                cleaning_days = 180

                cleaning_time = clear_time_format(current_time, cleaning_days, "%Y%m%d")
                print(cleaning_time)
                clear.super_statistics(cleaning_time)
            elif cleaning_method == "agent_sketch_list":
                # agent_sketch_list，默认清理90天前的
                cleaning_days = 90

                cleaning_time = clear_time_format(current_time, cleaning_days, "%Y%m%d")
                print(cleaning_time)
                clear.agent_sketch_list(cleaning_time)
            elif cleaning_method == "rs_dwd_user_account_currency_detail_h":
                # rs_dwd_user_account_currency_detail_h，默认清理365天前的
                cleaning_days = 365

                cleaning_time = clear_time_format(current_time, cleaning_days, "%Y%m%d")
                print(cleaning_time)
                clear.rs_dwd_user_account_currency_detail_h(cleaning_time)
            elif cleaning_method == "dp_edm_send_log":
                # dp_edm_send_log，默认清理90天前的
                cleaning_days = 90

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.dp_edm_send_log(cleaning_time)
            elif cleaning_method == "contract_record":
                # contract_record，默认清理180天前的
                cleaning_days = 180

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.contract_record(cleaning_time)
            elif cleaning_method == "t_D_Trade":
                # t_D_Trade，默认清理180天前的
                cleaning_days = 180

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.t_D_Trade(cleaning_time)
            elif cleaning_method == "t_D_AccountDetail":
                # t_D_AccountDetail，默认清理180天前的
                cleaning_days = 180

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.t_D_AccountDetail(cleaning_time)
            elif cleaning_method == "t_D_FinishOrder":
                # t_D_FinishOrder，默认清理180天前的
                cleaning_days = 180

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.t_D_FinishOrder(cleaning_time)
            elif cleaning_method == "t_D_FinishTriggerOrder":
                # t_D_FinishTriggerOrder，默认清理180天前的
                cleaning_days = 180

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.t_D_FinishTriggerOrder(cleaning_time)
            elif cleaning_method == "t_D_FinishPosition":
                # t_D_FinishPosition，默认清理180天前的
                cleaning_days = 180

                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)
                print(cleaning_time)
                clear.t_D_FinishPosition(cleaning_time)
            elif cleaning_method == "partner_record":
                # partner_record，默认清理60天前的
                cleaning_days = 60
                cleaning_time = int(time.time() - cleaning_days * 24 * 60 * 60)

                result = None
                start_temp_cleaning_time = cleaning_time - 300
                end_temp_cleaning_time = cleaning_time

                while result is None:
                    querySql = "select * from partner_record where create_time >= %s and create_time <= %s limit 1;"
                    cursor.execute(querySql, (start_temp_cleaning_time, end_temp_cleaning_time))
                    result = cursor.fetchone()

                    end_temp_cleaning_time = start_temp_cleaning_time
                    start_temp_cleaning_time = start_temp_cleaning_time - 300

                if result["create_time"] <= cleaning_time:
                    if result is not None:
                        print("result", result["id"], result["create_time"], cleaning_time)
                        clear.partner_record(result["id"])

