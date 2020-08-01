import sqlite3
from spyne import Application, ServiceBase, Unicode, rpc, ComplexModel, Float, Enum, Date
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from spyne import Integer, Array
import logging
from spyne.model.primitive import Mandatory
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.web.wsgi import WSGIResource
import json
import psycopg2
import pyodbc
import re

def check_postgres_query(query, host, database, user_name, stand=False):
    conn = psycopg2.connect(database=database, user="postgres", password=1, host=host, port="5432")
    cursor = conn.cursor()
    rows = cursor.execute('select "Name" from "NewSchema"."Nomenclature" where "Type" = ' + "'Table'")
    rows = cursor.fetchall()

    list_tables = []

    for i in range(len(rows)):
        list_tables.append(str(rows[i]))
        list_tables[i] = list_tables[i].replace("\'", "").replace("(", "").replace(")", "").replace(",", "")
        list_tables[i] = '\\b{0}\\b'.format(list_tables[i])

    check_user_group = cursor.execute(
        'select n1."Name" from "NewSchema"."Nomenclature" n1 join "NewSchema"."Link" l1 on n1."ID" = l1."ID_Parent" join "NewSchema"."Nomenclature" n2 on n2."ID" = l1."ID_Child" where n1."Type" = ' + "'Group'" + ' and ' + 'n2."Name" = ' + "'{0}'".format(
            user_name))
    check_user_group = cursor.fetchone()
    check_user_group = str(check_user_group).replace("\'", "").replace("(", "").replace(")", "").replace(",", "")

    list_query_table = []

    for i in list_tables:
        perem = re.findall(i, query)
        if len(perem) != 0:
            list_query_table.append(perem[0])

    if check_user_group != 'None':

        rul_list_group = []

        for j in list_query_table:
            checkrulesingroup = cursor.execute(
                'select a1."Atr" from "NewSchema"."Nomenclature" n1 join "NewSchema"."Link" l1 on n1."ID" = l1."ID_Parent" join "NewSchema"."Nomenclature" n2 on n2."ID" = l1."ID_Child" join "NewSchema"."AttributesLink" a1 on a1."ID_Link" = l1."ID" where n1."Name" = ' + "'{0}'".format(
                    check_user_group) + ' and ' + 'n2."Name" = ' + "'{0}'".format(j))

            checkrulesingroup = cursor.fetchone()
            checkrulesingroup = str(checkrulesingroup).replace("(", "").replace(", )", "").replace(",)",
                                                                                                   "")  # Десерилизация
            rul_list_group.append(checkrulesingroup)

        tables_into_query = ""

        for i in list_query_table:
            if len(list_query_table) == 1:
                tables_into_query += '"' + i + '"' + " "
            else:
                tables_into_query += '"' + i + '"' + ","

        tables_into_query = tables_into_query[:-1]

        d = dict.fromkeys(list_query_table)
        temp = 0
        for i in list_query_table:
            d[i] = rul_list_group[temp]
            if d[i] == 'None':
                d[i] = 0
            temp += 1

        query1 = query.lower()

    else:
        rul_list_us = []
        for i in list_query_table:
            checkrules = cursor.execute(
                'select a1."Atr" from "NewSchema"."Nomenclature" n1 join "NewSchema"."Link" l1 on n1."ID" = l1."ID_Parent" join "NewSchema"."Nomenclature" n2 on n2."ID" = l1."ID_Child" join "NewSchema"."AttributesLink" a1 on a1."ID_Link" = l1."ID" where n1."Name" = ' + "'{0}'".format(
                    user_name) + ' and ' + 'n2."Name" = ' + "'{0}'".format(i))

            checkrules = cursor.fetchone()
            checkrules = str(checkrules).replace("(", "").replace(", )", "").replace(",)", "")  # Десерилизация
            rul_list_us.append(checkrules)

        tables_into_query = ""

        for i in list_query_table:
            if len(list_query_table) == 1:
                tables_into_query += '"' + i + '"' + " "
            else:
                tables_into_query += '"' + i + '"' + ","

        tables_into_query = tables_into_query[:-1]

        d = dict.fromkeys(list_query_table)
        temp = 0
        for i in list_query_table:
            d[i] = rul_list_us[temp]
            if d[i] == 'None':
                d[i] = 0
            temp += 1

        query1 = query.lower()

        user_message_list = []

    flag = True
    for tab, rules in d.items():
        if int(rules) == 0:
            flag = False
            user_message_list.append("У вас нет прав для таблицы {0}".format(tab))

        if ((query1.find("insert", 0, 6) != -1) and (2 > int(rules) > 0)) or (
                (query1.find("update", 0, 6) != -1) and (2 > int(rules) > 0)):
            flag = False
            user_message_list.append(
                "У вас недостаточно прав для добавления или изменения данных таблицы {0}".format(tab))

        if (query1.find("delete", 0, 6) != -1) and (2 > int(rules) > 0):
            flag = False
            user_message_list.append(
                "У вас недостаточно прав для удаления данных из таблицы {0}".format(tab))

    if flag:

        if query1.find("select", 0, 6) != -1:
            if stand == False:
                someString = ""
                cursor.execute(query)
                for row in cursor.fetchall():
                    for podrow in row:
                        someString += str(podrow) + ' '
                    someString += '\n'
                return someString
            if stand == True:
                cursor.execute(query)
                return cursor.fetchall()

        if query1.find("insert", 0, 6) != -1:
            cursor.execute(query)
            conn.commit()
            user_message_list.append("Данные добавлены успешно")

        if query1.find("update", 0, 6) != -1:
            cursor.execute(query)
            conn.commit()
            user_message_list.append("Данные изменены успешно")

        if query1.find("delete", 0, 6) != -1:
            cursor.execute(query)
            conn.commit()
            user_message_list.append("Удаление произошло успешно")

    ret = ""
    for i in user_message_list:
        ret += i + "|"
    return ret
def check_ms_query(query, server, database, user_name,stand=False):
    query1 = query

    connection_string = 'Driver={SQL Server Native Client 11.0};' + '{0};'.format(server) + 'Database={0};'.format(
        database) + 'Trusted_Connection=yes;'
    conn = pyodbc.connect(connection_string)

    query = query.lower()
    cursor = conn.cursor()
    rows = cursor.execute(
        "select Name from Nomenclature where Type = 'Table'").fetchall()

    list_tables = []

    for i in range(len(rows)):
        list_tables.append(str(rows[i]))
        list_tables[i] = list_tables[i].replace("\'", "").replace("(", "").replace(")", "").replace(", ",
                                                                                                    "").lower()
        list_tables[i] = '\\b{0}\\b'.format(list_tables[i])

    check_user_group = cursor.execute(
        "select n1.Name from Nomenclature n1 join Link l1 on n1.ID = l1.ID_Parent  join Nomenclature n2 on n2.ID = l1.ID_Child where n1.Type = 'Group' and n2.Name = '{0}'".format(
            user_name))
    check_user_group = cursor.fetchone()
    check_user_group = str(check_user_group).replace("\'", "").replace("(", "").replace(")", "").replace(", ", "")

    list_query_table = []

    for i in list_tables:
        perem = re.findall(i, query.lower())
        if len(perem) != 0:
            list_query_table.append(perem[0])

    if check_user_group != 'None':
        rul_list_group = []
        for j in list_query_table:
            checkrulesingroup = cursor.execute(
                "select a1.Atr from Nomenclature n1 join Link l1 on n1.ID = l1.ID_Parent join Nomenclature n2 on n2.ID = l1.ID_Child join AttributesLink a1 on a1.ID_Link = l1.ID where n1.Name = '{0}' and n2.Name = '{1}'".format(
                    check_user_group, j))
            checkrulesingroup = cursor.fetchone()
            checkrulesingroup = str(checkrulesingroup).replace("(", "").replace(", )", "")  # Десерилизация
            rul_list_group.append(checkrulesingroup)

        tables_into_query = ""

        for i in list_query_table:
            if len(list_query_table) == 1:
                tables_into_query += i + " "
            else:
                tables_into_query += i + ","

        tables_into_query = tables_into_query[:-1]

        d = dict.fromkeys(list_query_table)
        temp = 0
        for i in list_query_table:
            d[i] = rul_list_group[temp]
            if d[i] == 'None':
                d[i] = 0
            temp += 1

    else:
        rul_list_us = []
        for i in list_query_table:
            checkrules = cursor.execute(
                "select a1.Atr from Nomenclature n1 join Link l1 on n1.ID = l1.ID_Parent join Nomenclature n2 on n2.ID = l1.ID_Child join AttributesLink a1 on a1.ID_Link = l1.ID where n1.Name = '{0}' and n2.Name = '{1}'".format(
                    user_name, i))
            checkrules = cursor.fetchone()
            checkrules = str(checkrules).replace("(", "").replace(", )", "")  # Десерилизация
            rul_list_us.append(checkrules)

        tables_into_query = ""

        for i in list_query_table:
            if len(list_query_table) == 1:
                tables_into_query += i + " "
            else:
                tables_into_query += i + ","

        tables_into_query = tables_into_query[:-1]

        d = dict.fromkeys(list_query_table)
        temp = 0
        for i in list_query_table:
            d[i] = rul_list_us[temp]
            if d[i] == 'None':
                d[i] = 0
            temp += 1

    user_message_list = []
    flag = True
    for tab, rules in d.items():
        if int(rules) == 0:
            flag = False
            user_message_list.append("У вас нет прав для таблицы {0}".format(tab))

        if ((query.find("insert", 0, 6) != -1) and (2 > int(rules) > 0)) or (
                (query.find("update", 0, 6) != -1) and (2 > int(rules) > 0)):
            flag = False
            user_message_list.append(
                "У вас недостаточно прав для добавления или изменения данных таблицы {0}".format(tab))

        if (query.find("delete", 0, 6) != -1) and (2 > int(rules) > 0):
            flag = False
            user_message_list.append("У вас недостаточно прав для удаления данных из таблицы {0}".format(tab))

    if flag:
        if query.find("select", 0, 6) != -1:
            if stand == False:
                someString = ""
                cursor.execute(query1)
                for row in cursor.fetchall():
                    for podrow in row:
                        someString += str(podrow) + ' '
                    someString += '\n'
                return someString
            if stand == True:
                cursor.execute(query1)
                return cursor.fetchall()

            return cursor.fetchall()

        if query.find("insert", 0, 6) != -1:
            cursor.execute(query1)
            conn.commit()
            user_message_list.append("Данные добавлены успешно")

        if query.find("update", 0, 6) != -1:
            cursor.execute(query1)
            conn.commit()
            user_message_list.append("Данные изменены успешно")

        if query.find("delete", 0, 6) != -1:
            cursor.execute(query1)
            conn.commit()
            user_message_list.append("Удаление произошло успешно")

    ret = ""
    for i in user_message_list:
        ret += i + "|"
    return ret

class Task(ComplexModel):
    ID = Integer
    first_name = Unicode
    last_name = Unicode


class TaskList(ComplexModel):
    alltasks = Array(Task)


class RequestHeader(ComplexModel):
    user_name = Mandatory.String


class Query(ComplexModel):
    first_name = Unicode
    last_name = Unicode
    status = Unicode
    sallary = Float


class JSONrecord(ComplexModel):
    name = Unicode
    query = Unicode
    description = Unicode
    created = Unicode


class JSONrecords(ComplexModel):
    records = Array(JSONrecord)


class QueryList(ComplexModel):
    human = Array(Query)


class Dolgnost(ComplexModel):
    ID = Integer
    dolgnost_name = Unicode
    oklad = Float
    warning = Unicode

class Dolgnostarr(ComplexModel):
    arr = Array(Dolgnost)


class Employee_doc(ComplexModel):
    ID = Integer
    pasport_seria = Unicode
    pasport_num = Unicode
    snils = Unicode
    inn = Unicode
    police = Unicode
    warning = Unicode

class Employee_docArr(ComplexModel):
    arr = Array(Employee_doc)


class Employee_info(ComplexModel):
    ID = Integer
    lastName = Unicode
    firstName = Unicode
    middleName = Unicode
    adress = Unicode
    phone = Unicode
    birthday = Date
    id_doc = Integer
    warning = Unicode

class Employee_infoArr(ComplexModel):
    arr = Array(Employee_info)


class Otdel(ComplexModel):
    ID = Integer
    otdel_name = Unicode
    otdel_code = Unicode
    warning = Unicode

class OtdelArr(ComplexModel):
    arr = Array(Otdel)


class Work_place_of_employee(ComplexModel):
    ID = Integer
    id_employee = Integer
    id_dolgnost = Integer
    id_otdel = Integer
    warning = Unicode

class Work_place_of_employeeArr(ComplexModel):
    arr = Array(Work_place_of_employee)


class Work_place_join(ComplexModel):
    ID = Integer
    lastName = Unicode
    firstName = Unicode
    middleName = Unicode
    adress = Unicode
    phone = Unicode
    birthday = Date
    id_doc = Integer
    id_dolgnost = Integer
    id_otdel = Integer
    warning = Unicode

class Work_place_joinArr(ComplexModel):
    arr = Array(Work_place_join)


class Work_place_join1(ComplexModel):
    ID = Integer
    id_employee = Integer
    dolgnost_name = Unicode
    oklad = Float
    id_otdel = Integer
    warning = Unicode

class Work_place_join1Arr(ComplexModel):
    arr = Array(Work_place_join1)


class Work_place_join2(ComplexModel):
    ID = Integer
    id_employee = Integer
    id_dolgnost = Integer
    otdel_name = Unicode
    otdel_code = Unicode
    warning = Unicode

class Work_place_join2Arr(ComplexModel):
    arr = Array(Work_place_join2)


class Work_place_join3(ComplexModel):
    ID = Integer
    lastName = Unicode
    firstName = Unicode
    middleName = Unicode
    adress = Unicode
    phone = Unicode
    birthday = Date
    id_doc = Integer
    dolgnost_name = Unicode
    oklad = Float
    otdel_name = Unicode
    otdel_code = Unicode
    warning = Unicode

class Work_place_join3Arr(ComplexModel):
    arr = Array(Work_place_join3)


class All(ComplexModel):
    ID = Integer
    lastName = Unicode
    firstName = Unicode
    middleName = Unicode
    adress = Unicode
    phone = Unicode
    birthday = Date
    pasport_seria = Unicode
    pasport_num = Unicode
    snils = Unicode
    inn = Unicode
    police = Unicode
    dolgnost_name = Unicode
    oklad = Float
    otdel_name = Unicode
    otdel_code = Unicode
    warning = Unicode


class AllArr(ComplexModel):
    arr = Array(All)


class Employee_all(ComplexModel):
    ID = Integer
    lastName = Unicode
    firstName = Unicode
    middleName = Unicode
    adress = Unicode
    phone = Unicode
    birthday = Date
    pasport_seria = Unicode
    pasport_num = Unicode
    snils = Unicode
    inn = Unicode
    police = Unicode
    warning = Unicode

class Employee_allArr(ComplexModel):
    arr = Array(Employee_all)


class Designer(ComplexModel):
    _type_info = {
        'orderby': Unicode,
        'where': Unicode
    }


def jsonDe(name, opers=None):
    if opers is None:
        with open('add.json') as add:
            template = json.load(add)
        record = template[name]
        return record['query']
    elif not (opers is None):
        with open('add.json') as add:
            template = json.load(add)
        record = template[name]
        query = record['query']
        query = '{0} {1}'.format(query, opers)
        return query


def designer(query, WHERE=None, ORDER_BY=None):
    if not (WHERE == None):
        query = '{0} {1}'.format(query, WHERE)
    if not (ORDER_BY == None):
        query = '{0} {1}'.format(query, ORDER_BY)


class StandartQuery(ServiceBase):
    __in_header__ = RequestHeader

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=Dolgnostarr)
    def dolgnost(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'SELECT "ID", "Dolgnost_name", "Oklad" FROM "NewSchema"."Dolgnost" '
        if subd == 'MS':
            query = 'SELECT "ID", "Dolgnost_name", "Oklad" FROM "Dolgnost" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby

        dolgArr = Dolgnostarr()
        dolgArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            dolg = Dolgnost()
            dolg.warning = rows
            dolgArr.arr.append(dolg)
            return dolgArr
        for row in rows:
            dolg = Dolgnost()
            dolg.ID = row[0]
            dolg.dolgnost_name = row[1]
            dolg.oklad = row[2]
            dolgArr.arr.append(dolg)
        return dolgArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=Employee_docArr)
    def employee_doc(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'SELECT "ID", "Pasport_seria", "Pasport_num", "Snils", "Inn", "Police" FROM "NewSchema"."Employee_doc" '

        if subd == 'MS':
            query = 'SELECT "ID", "Pasport_seria", "Pasport_num", "Snils", "Inn", "Police" FROM "Employee_doc" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        empArr = Employee_docArr()
        empArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name,True)
        if isinstance(rows,str):
            emp = Employee_doc()
            emp.warning = rows
            empArr.arr.append(emp)
            return empArr
        for row in rows:
            emp = Employee_doc()
            emp.ID = row[0]
            emp.pasport_seria = row[1]
            emp.pasport_num = row[2]
            emp.snils = row[3]
            emp.inn = row[4]
            emp.police = row[5]
            empArr.arr.append(emp)
        return empArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode,  _returns=Employee_infoArr)
    def employee_info(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'SELECT "ID", "LastName", "FirstName", "MiddleName", "Adress", "Phone", "Birthday", "ID_doc" FROM "NewSchema"."Employee_info" '

        if subd == 'MS':
            query = 'SELECT "ID", "LastName", "FirstName", "MiddleName", "Adress", "Phone", "Birthday", "ID_doc" FROM "Employee_info" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        empIArr = Employee_infoArr()
        empIArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query,server,database,ctx.in_header.user_name,True)
        if isinstance(rows,str):
            empI = Employee_info()
            empI.warning = rows
            empIArr.arr.append(empI)
            return empIArr
        for row in rows:
            empI = Employee_info()
            empI.ID = row[0]
            empI.lastName = row[1]
            empI.firstName = row[2]
            empI.middleName = row[3]
            empI.adress = row[4]
            empI.phone = row[5]
            empI.birthday = row[6]
            empI.id_doc = row[7]
            empIArr.arr.append(empI)
        return empIArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=Employee_allArr)
    def employee_all(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'SELECT e1."ID", e1."LastName", e1."FirstName", e1."MiddleName", e1."Adress", e1."Phone", e1."Birthday", e2."Pasport_seria",e2."Pasport_num",e2."Snils",e2."Inn",e2."Police" FROM "NewSchema"."Employee_info" e1, "NewSchema"."Employee_doc" e2 where e1."ID_doc" = e2."ID" '

        if subd == 'MS':
            query = 'SELECT e1."ID", e1."LastName", e1."FirstName", e1."MiddleName", e1."Adress", e1."Phone", e1."Birthday", e2."Pasport_seria",e2."Pasport_num",e2."Snils",e2."Inn",e2."Police" FROM "Employee_info" e1, "Employee_doc" e2 where e1."ID_doc" = e2."ID" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        empIArr = Employee_allArr()
        empIArr.arr = []

        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            empI = Employee_all()
            empI.warning = rows
            empIArr.arr.append(empI)
            return empIArr
        for row in rows:
            empI = Employee_all()
            empI.ID = row[0]
            empI.lastName = row[1]
            empI.firstName = row[2]
            empI.middleName = row[3]
            empI.adress = row[4]
            empI.phone = row[5]
            empI.birthday = row[6]
            empI.pasport_seria = row[7]
            empI.pasport_num = row[8]
            empI.snils = row[9]
            empI.inn = row[10]
            empI.police = row[11]
            empIArr.arr.append(empI)
        return empIArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=OtdelArr)
    def otdel(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'SELECT "ID", "Otdel_name", "Otdel_code" FROM "NewSchema"."Otdel" '

        if subd == 'MS':
            query = 'SELECT "ID", "Otdel_name", "Otdel_code" FROM "Otdel" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        otdArr = OtdelArr()
        otdArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            otd = Otdel()
            otd.warning = rows
            otdArr.arr.append(otd)
            return otdArr
        for row in rows:
            otd = Otdel()
            otd.ID = row[0]
            otd.otdel_name = row[1]
            otd.otdel_code = row[2]
            otdArr.arr.append(otd)
        return otdArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=Work_place_of_employeeArr)
    def work_place(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'SELECT "ID", "ID_employee", "ID_dolgnost", "ID_otdel" FROM "NewSchema"."Work_place_of_emploee" '

        if subd == 'MS':
            query = 'SELECT "ID", "ID_employee", "ID_dolgnost", "ID_otdel" FROM "Work_place_of_emploee" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        workArr = Work_place_of_employeeArr()
        workArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            work = Work_place_of_employee()
            work.warning = rows
            workArr.arr.append(work)
            return workArr
        for row in rows:
            work = Work_place_of_employee()
            work.ID = row[0]
            work.id_employee = row[1]
            work.id_dolgnost = row[2]
            work.id_otdel = row[3]
            workArr.arr.append(work)
        return workArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=Work_place_joinArr)
    def work_place_join_emInfo(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'select w1."ID",e1."LastName",e1."FirstName",e1."MiddleName",e1."Adress",e1."Phone",e1."Birthday",e1."ID_doc", w1."ID_dolgnost", w1."ID_otdel" from "NewSchema"."Work_place_of_emploee" w1,"NewSchema"."Employee_info" e1 where w1."ID_employee" = e1."ID" '

        if subd == 'MS':
            query = 'select w1."ID",e1."LastName",e1."FirstName",e1."MiddleName",e1."Adress",e1."Phone",e1."Birthday",e1."ID_doc", w1."ID_dolgnost", w1."ID_otdel" from "Work_place_of_emploee" w1, "Employee_info" e1 where w1."ID_employee" = e1."ID" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        workArr = Work_place_joinArr()
        workArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            work = Work_place_join()
            work.warning = rows
            workArr.arr.append(work)
            return workArr
        for row in rows:
            work = Work_place_join()
            work.ID = row[0]
            work.lastName = row[1]
            work.firstName = row[2]
            work.middleName = row[3]
            work.adress = row[4]
            work.phone = row[5]
            work.birthday = row[6]
            work.id_doc = row[7]
            work.id_dolgnost = row[8]
            work.id_otdel = row[9]
            workArr.arr.append(work)
        return workArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=Work_place_join1Arr)
    def work_place_join_dolgnost(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'select w1."ID",w1."ID_employee",d1."Dolgnost_name",d1."Oklad",w1."ID_otdel" from "NewSchema"."Work_place_of_emploee" w1,"NewSchema"."Dolgnost" d1 where w1."ID_dolgnost" = d1."ID" '

        if subd == 'MS':
            query = 'select w1."ID",w1."ID_employee",d1."Dolgnost_name",d1."Oklad",w1."ID_otdel" from "Work_place_of_emploee" w1, "Dolgnost" d1 where w1."ID_dolgnost" = d1."ID" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        workArr = Work_place_join1Arr()
        workArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            work = Work_place_join1()
            work.warning = rows
            workArr.arr.append(work)
            return workArr
        for row in rows:
            work = Work_place_join1()
            work.ID = row[0]
            work.id_employee = row[1]
            work.dolgnost_name = row[2]
            work.oklad = row[3]
            work.id_otdel = row[4]
            workArr.arr.append(work)
        return workArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=Work_place_join2Arr)
    def work_place_join_otdel(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'select w1."ID",w1."ID_employee",w1."ID_dolgnost",o1."Otdel_name",o1."Otdel_code" from "NewSchema"."Work_place_of_emploee" w1,"NewSchema"."Otdel" o1 where w1."ID_otdel" = o1."ID" '

        if subd == 'MS':
            query = 'select w1."ID",w1."ID_employee",w1."ID_dolgnost",o1."Otdel_name",o1."Otdel_code" from "Work_place_of_emploee" w1, "Otdel" o1 where w1."ID_otdel" = o1."ID" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        workArr = Work_place_join2Arr()
        workArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            work = Work_place_join2()
            work.warning = rows
            workArr.arr.append(work)
            return workArr
        for row in rows:
            work = Work_place_join2()
            work.ID = row[0]
            work.id_employee = row[1]
            work.id_dolgnost = row[2]
            work.otdel_name = row[3]
            work.otdel_code = row[4]
            workArr.arr.append(work)
        return workArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=Work_place_join3Arr)
    def work_place_join_all(ctx,database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'select w1."ID",e1."LastName",e1."FirstName",e1."MiddleName",e1."Adress",e1."Phone",e1."Birthday",e1."ID_doc",d1."Dolgnost_name",d1."Oklad", o1."Otdel_name",o1."Otdel_code" from "NewSchema"."Work_place_of_emploee" w1,"NewSchema"."Employee_info" e1,"NewSchema"."Dolgnost" d1,"NewSchema"."Otdel" o1  where w1."ID_employee" = e1."ID" and w1."ID_otdel" = o1."ID" and w1."ID_dolgnost" = d1."ID" '

        if subd == 'MS':
            query = 'select w1."ID",e1."LastName",e1."FirstName",e1."MiddleName",e1."Adress",e1."Phone",e1."Birthday",e1."ID_doc",d1."Dolgnost_name",d1."Oklad", o1."Otdel_name",o1."Otdel_code" from "Work_place_of_emploee" w1, "Employee_info" e1, "Dolgnost" d1, "Otdel" o1  where w1."ID_employee" = e1."ID" and w1."ID_otdel" = o1."ID" and w1."ID_dolgnost" = d1."ID" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        workArr = Work_place_join3Arr()
        workArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            work = Work_place_join3()
            work.warning = rows
            workArr.arr.append(work)
            return workArr
        for row in rows:
            work = Work_place_join3()
            work.ID = row[0]
            work.lastName = row[1]
            work.firstName = row[2]
            work.middleName = row[3]
            work.adress = row[4]
            work.phone = row[5]
            work.birthday = row[6]
            work.id_doc = row[7]
            work.dolgnost_name = row[8]
            work.oklad = row[9]
            work.otdel_name = row[10]
            work.otdel_code = row[11]
            workArr.arr.append(work)
        return workArr

    @rpc(Unicode, Unicode,Designer, Unicode,Unicode, _returns=AllArr)
    def all(ctx, database, subd, des=None,server = None,host=None):
        query = ''
        if subd == 'PG':
            query = 'select w1."ID",e1."LastName",e1."FirstName",e1."MiddleName",e1."Adress",e1."Phone",e1."Birthday",e2."Pasport_seria",e2."Pasport_num",e2."Snils",e2."Inn",e2."Police",d1."Dolgnost_name",d1."Oklad", o1."Otdel_name",o1."Otdel_code"  from "NewSchema"."Work_place_of_emploee" w1,"NewSchema"."Employee_info" e1,"NewSchema"."Dolgnost" d1,"NewSchema"."Otdel" o1, "NewSchema"."Employee_doc" e2  where w1."ID_employee" = e1."ID" and w1."ID_otdel" = o1."ID" and w1."ID_dolgnost" = d1."ID" and e1."ID_doc" = e2."ID" '

        if subd == 'MS':
            query = 'select w1."ID",e1."LastName",e1."FirstName",e1."MiddleName",e1."Adress",e1."Phone",e1."Birthday",e2."Pasport_seria",e2."Pasport_num",e2."Snils",e2."Inn",e2."Police",d1."Dolgnost_name",d1."Oklad", o1."Otdel_name",o1."Otdel_code"  from "Work_place_of_emploee" w1, "Employee_info" e1, "Dolgnost" d1, "Otdel" o1, "Employee_doc" e2  where w1."ID_employee" = e1."ID" and w1."ID_otdel" = o1."ID" and w1."ID_dolgnost" = d1."ID" and e1."ID_doc" = e2."ID" '
        if not des is None:
            if not (des.where is None):
                query += 'and' + ' ' + des.where
            if not (des.orderby is None):
                query += 'order by' + ' ' + des.orderby
        workArr = AllArr()
        workArr.arr = []
        if subd == 'PG':
            rows = check_postgres_query(query, host, database, ctx.in_header.user_name, True)
        if subd == 'MS':
            rows = check_ms_query(query, server, database, ctx.in_header.user_name, True)
        if isinstance(rows,str):
            work = All()
            work.warning = rows
            workArr.arr.append(work)
            return workArr
        for row in rows:
            work = All()
            work.ID = row[0]
            work.lastName = row[1]
            work.firstName = row[2]
            work.middleName = row[3]
            work.adress = row[4]
            work.phone = row[5]
            work.birthday = row[6]
            work.pasport_seria = row[7]
            work.pasport_num = row[8]
            work.snils = row[9]
            work.inn = row[10]
            work.police = row[11]
            work.dolgnost_name = row[12]
            work.oklad = row[13]
            work.otdel_name = row[14]
            work.otdel_code = row[15]
            workArr.arr.append(work)
        return workArr




class JsonService(ServiceBase):
    __in_header__ = RequestHeader

    @rpc(Unicode, Unicode, Unicode, Unicode, Unicode, _returns=Unicode)
    def getQuery(ctx, name,  database, operation=None,host=None,server=None):
        with open('add.json') as add:
            template = json.load(add)
        record = template[name]
        if record['subd']=='PG':
            return check_postgres_query(jsonDe(name, operation), host, database, user_name=ctx.in_header.user_name)
        if record['subd']=='MS':
            return check_ms_query(jsonDe(name, operation),server,database,user_name=ctx.in_header.user_name)

    @rpc(Unicode, Unicode,Unicode, Unicode, _returns=Unicode)
    def JSONgeneration(ctx, name, queryFrom,subd, description=None):
        if description is None:
            description = "Нет описания"
        json_string = '{"' + name + '":{"query":"' + queryFrom + '","subd":"'+subd+'","user": "' + ctx.in_header.user_name + '","description": "' + description + '"}}'
        with open('add.json') as add:
            template = json.load(add)
        template.update(json.loads(json_string))
        with open("add.json", "w") as write_file:
            json.dump(template, write_file)

        return "Запись добавлена"

    @rpc(Unicode, _returns=Unicode)
    def JSONdelete(ctx, name):
        user = ctx.in_header.user_name
        with open('add.json') as add:
            template = json.load(add)
        JSONrecord = template[name]
        if JSONrecord['user'] == user:
            del template[name]
            result = "Запись удалена"
            with open("add.json", "w") as write_file:
                json.dump(template, write_file)
        else:
            result = "Запись не удалена"
        return result

    @rpc(_returns=JSONrecords)
    def JSONshow(ctx):
        with open('add.json') as add:
            template = json.load(add)
        records = JSONrecords()
        records.records = []
        for i in template:
            record = JSONrecord()
            record.name = i
            record.query = template[i]['query']
            record.description = template[i]['description']
            record.created = template[i]['user']
            records.records.append(record)
        return records




print("Введите IP:")
HOST = input()
print("Введите порт:")
PORT = int(input())

application = Application(services=[JsonService, StandartQuery], tns='Server-API',
                          in_protocol=Soap11(), out_protocol=Soap11())
#HOST = '192.168.1.44'
#PORT = 8000


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)
logging.getLogger('twisted').setLevel(logging.DEBUG)
wsgi_app = WsgiApplication(application)

resource = WSGIResource(reactor, reactor, wsgi_app)
site = Site(resource)
reactor.listenTCP(PORT, site, interface=HOST)
logging.info("listening to http://{0}:{1}".format(HOST,str(PORT)))
logging.info("wsdl is at: http://{0}:{1}/?wsdl".format(HOST,str(PORT)))
reactor.run()
