#!/home/q/python27/bin/python
# -*- coding: utf-8 -*-
# created by: yinggang.zhao@qunar.com
# Feedback and improvements are welcome.
# you can also mail to me: nettedfish@qq.com
# date: 2014-01-15 11:56

import os
import sys
import logging
import logging.handlers
import socket
import re
import ConfigParser
import zookeeper
import mysql.connector

config = ConfigParser.RawConfigParser()
root_dir = os.path.dirname(os.path.abspath(__file__)) + "/.."
config.read(root_dir + "/conf/meta.conf")
LOG_FILE = root_dir + '/log/reconfig.log'
sys.path.append(root_dir + "/lib/")
from send_mail import send_mail
import zkclient
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1073741824, backupCount=1)
fmt = '%(asctime)s - [%(levelname)s] - [%(name)s/%(filename)s: %(lineno)d] - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger('reconfig_logger')
logger.addHandler(handler)
logger.setLevel(logging.INFO)
mail_logger = logging.getLogger('mail_logger')
mail_logger.addHandler(handler)
mail_logger.setLevel(logging.DEBUG)
py_zklogger = logging.getLogger('py_zkclient')
py_zklogger.addHandler(handler)
py_zklogger.setLevel(logging.INFO)

zk_server_list = config.get("meta", "zk_server_list")
mail_to = config.get("meta", "mail_to").split(",")
config_server_hosts = [tuple(item.split(":")) for item in config.get('config_server', 'hosts').split("_")]
config_server_username = config.get('config_server', 'username')
config_server_password = config.get('config_server', 'password')

R_ERROR = -1
R_OK = 0
R_NOT_OK = 1 #NOT OK �����Ǵ���


def get_domain_name(ip):
    """
    ȡip������������������󣬾ͷ���ip
    """
    try:
        domain_name = socket.gethostbyaddr(ip)[0]
        return domain_name.replace('.qunar.com', '')
    except:
        return ip

def make_config_server_connection():
    """
    �����������ĵ����ݿ�����
    ע��autocommit��False
    ����и��£�����������ǵ���ʾcommit��rollback
    """
    for (host,port) in config_server_hosts:
        try:
            config = {
                'user': config_server_username,
                'password': config_server_password,
                'host': host,
                'port': int(port),
                'database': 'config_server',
                'charset': 'utf8mb4',
                'connection_timeout': 2.0,
                'autocommit': False
            }
            return mysql.connector.Connect(**config)
        except mysql.connector.Error as e:
            logger.error('��ȡ�����������ݿ�����ʧ��, err: %s', str(e))
    return None

def close_db_cursor_and_connection(cur, conn):
    try:
        if cur:
            cur.close()
    except:
        pass
    try:
        if conn:
            conn.close()
    except:
        pass

def is_version_2(from_ip, from_port):
    """
    �ж�from_ip, from_port redisʵ�����Ƿ�������������
    ����ڣ�����v2�汾
    ������ڣ������ϰ汾
    ע���������������string
    port�ڱ�Ҫʱ��Ҫת��int
    """
    conn = make_config_server_connection()
    if not conn:
        return R_ERROR
    cur = None
    try:
        cur = conn.cursor()
        sql = ('SELECT count(*)'
               ' FROM redis_resource'
               ' WHERE cluster_ip=%s'
               ' AND cluster_port=%s')
        cur.execute(sql, (from_ip, int(from_port),))
        if not cur.with_rows:
            return R_NOT_OK
        else:
            rows = cur.fetchone()[0]
            if rows > 0:
                return R_OK
            else:
                return R_NOT_OK
    except mysql.connector.Error as e:
        logger.error('�������ݿ�ʱ�������쳣 err: %s', str(e))
        return R_ERROR
    finally:
        close_db_cursor_and_connection(cur, conn)

def update_namespace_zk_version(ns, msg=None):
    """
    ����zk��/redis/{ns}�汾
    ��zk֪ͨ�ͻ���,������������
    """
    zkc = None
    try:
        zkc = zkclient.ZkClient(zk_server_list)
        zpath = '/redis/{0}'.format(ns)
        set_ret = zkc.Set(zpath, msg, -1)
        if set_ret == zookeeper.OK:
            ok_msg = (
'namespace={0}, ����zk�ɹ�'
            ).format(ns)
            send_mail(mail_to, ok_msg)
            return R_OK
        else:
            err_msg = (
'namespace={0}, ����zkʧ�ܣ�����ֵ={1}'
            ).format(ns, set_ret)
            send_mail(mail_to, err_msg)
            return R_ERROR
    except Exception as e:
        ext_msg = (
'namespace={0}, ����zk�쳣, err:{1}'
        ).format(ns, str(e))
        logger.error(ext_msg)
        send_mail(mail_to, ext_msg)
        return R_ERROR

def v2_reconfig_process(from_ip, from_port, to_ip, to_port):
    """
    ע���������������string
    port�ڱ�Ҫʱ��Ҫת��int
    """
    conn = make_config_server_connection()
    if not conn:
        return R_ERROR
    # ȡ��from_ip, from_portʵ����Ϣ
    cur = conn.cursor()
    try:
        select_sql = (
'SELECT id,namespace,virtualname,cluster_ip,cluster_port,update_time'
' FROM redis_resource'
' WHERE cluster_ip=%s'
' AND cluster_port=%s'
        )
        cur.execute(select_sql, (from_ip, int(from_port),))
        if not cur.with_rows: #û���ҵ���¼������
            err_msg = (
'v2_reconfig_process() ���ݿ���û���ҵ�ip={ip}, port={port}'
'��redisʵ����Ϣ�����������Σ�գ���DBA���ϲ鿴!'
            ).format(ip=from_ip,port=int(from_port))
            logger.error(err_msg)
            send_mail(mail_to, err_msg)
            return R_ERROR
        else:
            # ��������£�ֻ��ȡ��һ��
            # ���´�ʵ����Ϣ
            rows = cur.fetchall()
            for cid,ns,vname,cip,cport,utime in rows:
                update_sql = (
'UPDATE redis_resource'
' SET cluster_ip=%s,cluster_port=%s'
' WHERE id=%s AND update_time=%s'
                )
                cur.execute(update_sql, (to_ip, int(to_port), cid, utime,))
                if cur.rowcount == 1: #�ɹ�
                    ok_msg = (
'v2_reconfig_process() �л��ɹ�.'
' {cname} {oip}:{oport}@{ns} �л���'
'{nip}:{nport}@{ns}').format(
                        oip=get_domain_name(cip),
                        oport=cport,
                        nip=get_domain_name(to_ip),
                        nport=to_port,
                        ns=ns,
                        cname=vname
                    )
                    conn.commit()
                    logger.info(ok_msg)
                    send_mail(mail_to, ok_msg)
                    #����zk
                    update_namespace_zk_version(ns, ok_msg)
                    return R_OK
                else: #ʧ��
                    err_msg = (
'v2_reconfig_process() �������ݿ�ʧ�ܣ����������Σ�գ���DBA���ϲ鿴!'
'from_ip={0}, from_port={1},'
'to_ip={2}, to_port={3}'
                    ).format(from_ip, from_port, to_ip, to_port)
                    conn.rollback()
                    logger.error(err_msg)
                    send_mail(mail_to, err_msg)
                    return R_ERROR
    except mysql.connector.Error as e:
        err_msg = 'v2_reconfig_process() �������ݿ�ʱ�������쳣 err: %s, ��鿴��', str(e)
        logger.error(err_msg)
        send_mail(mail_to, err_msg)
        return R_ERROR
    finally:
        close_db_cursor_and_connection(cur, conn)

def NodeReconfig(first_layer_node, sentinel_from_info, sentinel_to_info):
    zkc = zkclient.ZkClient(zk_server_list)
    if first_layer_node not in ("/redis", "/mysql"):
        logger.critical("��һ��ڵ������/redis��/mysql")
        sys.exit()
    second_layer_nodes = zkc.GetChildren(first_layer_node, None)
    logger.debug("��һ���ڵ�%s��ȡ�����ж����ڵ��б�:%s" % (first_layer_node, str(second_layer_nodes)))
    try:
        for second_layer_node in second_layer_nodes:
            # ������secondary_node�Ƿ���Ҫ������.Ҫ��������ζ�ſͻ��˵����ӳ���Ҫ�ؽ�
            secondary_node_need_update = 0
            third_layer_nodes = zkc.GetChildren(first_layer_node + "/" + second_layer_node, None)
            logger.debug("�Ӷ����ڵ�%s��ȡ�Ķ�Ӧ�����ڵ��б�Ϊ:%s" % ((str(second_layer_node),
                                                                     str(third_layer_nodes))))
            for third_layer_node in third_layer_nodes:
                if not re.match(r'cluster(\d+):\d+\.\d+\.\d+\.\d+:\1::\d+:\d+:\d+', third_layer_node):
                    info = "�ڵ�/redis/%s/%s·����ʽ���� ���޸�" % (second_layer_node, third_layer_node)
                    #logger.critical(info)
                    # ���Ի����£���ʽ���󣬾Ͳ��������ˣ�̫��
                    #send_mail(mail_to, info)

                if third_layer_node.startswith(sentinel_from_info):
                    # �����������ڵ���Ҫ������,һ����Ҫ������,���������κδ���,�������صĴ���,���ᵼ��zk�е���Ϣ
                    # ��sentinel��������Ϣ��һ��,��Ҫ�ϸ��Ų�
                    secondary_node_need_update = 1
                    from_node = third_layer_node
                    to_node = from_node.replace(sentinel_from_info, sentinel_to_info, 1)
                    # ����ڵ�ľ���·��
                    from_node_abs_path = first_layer_node + "/" + second_layer_node + "/" + from_node
                    to_node_abs_path = first_layer_node + "/" + second_layer_node + "/" + to_node
                    node_data = """#Comments created by redis sentinel reconfig.py. First create node:""" \
                                """ %s, then delete node: %s""" % (to_node_abs_path, from_node_abs_path)
                    try:
                        ret_create = zkc.Create(to_node_abs_path, node_data, 0)
                        # createĬ�Ϸ��ص��Ǵ�����·��
                        logger.info("�����ڵ�:%s�ɹ�" % (to_node_abs_path))
                    except zookeeper.NodeExistsException, e:
                        info = "redis�ڱ�����zookeeper�ڵ�%sʧ��. ������Ϣ:%s" % (to_node_abs_path, str(e))
                        logger.critical(info)
                        send_mail(mail_to, info)
                        sys.exit()
                    try:
                        ret_delete = zkc.Delete(from_node_abs_path)
                        # delete�ɹ�,�򷵻�OK
                        if ret_delete == zookeeper.OK:
                            logger.info("ɾ���ڵ�:%s,�����ɹ�. ����ֵ: %s" % (from_node_abs_path, str(ret_delete)))
                        if ret_delete == zookeeper.NONODE:
                            info = "ɾ���ڵ�:%s,����ʧ��.�ڵ㲻����.%s" % (from_node_abs_path, str(ret_delete))
                            logger.critical(info)
                            send_mail(mail_to, info)
                            sys.exit()
                    except Exception, e:
                        info = "ɾ���ڵ�:%s. ������������: %s" % (from_node_abs_path, str(e))
                        logger.critical(info)
                        send_mail(mail_to, info)
                        sys.exit()

            secondary_node_abs_path = first_layer_node + "/" + second_layer_node
            if secondary_node_need_update == 1:
                # ���ڶ����µ����нڵ��޸���Ϻ�,��ʼ���µڶ���ڵ�İ汾��,��Ϊ�ύ
                logger.warn("�����ڵ�%s���ӽڵ��Ѿ������仯,�����¸ö����ڵ�汾��" % secondary_node_abs_path)
                ret_set = zkc.Set(secondary_node_abs_path, "Standing on shoudlers of giants. QUNAR DBA!", -1)
                # ���������,zookeeper�᷵��zookeeper.OK,��ֵΪ0,��ʾ�ڵ㴴���ɹ�
                if ret_set == zookeeper.OK:
                    logger.info("���¶����ڵ�%s�汾�ųɹ�,����ֵ: %s" % (secondary_node_abs_path, str(ret_set)))
                    success_message = """�ڱ����������л�,zookeeper�Ķ����ڵ�%s��ƥ��%s�Ľڵ���ɹ��л���%s""" % (
                                                                                              secondary_node_abs_path,
                                                                                              sentinel_from_info,
                                                                                              sentinel_to_info)
                    logger.info(success_message + " �ڵ�%s�����failover�����Ѿ��������" % sentinel_from_info)
                    send_mail(mail_to, success_message)
                    # ��ǰ�κ�һ��redisʵ����ֻ����ദ��һ�������ڵ�֮�� ��Ϊzk����Ľڵ���ұȽ����� ����ֻ��
                    # ˳����� ����һ���ҵ�ƥ��Ľڵ� �Ͳ����ټ���������ȥ ��Ϊ������� ��ô��Ȼ��������
                    # ĳ��ʵ�����ֹ��Ϻ� ���ֻ����һ�������ڵ��ܵ�Ӱ�� ��˳����ߵ���һ���Ϳ����˳��� ˵��
                    # �漰�ýڵ�ı���Ѿ����
                    sys.exit()
                else:
                    info = "���¶����ڵ�%s�汾��ʧ��.����ֵ: %s" % (secondary_node_abs_path, str(ret_set))
                    logger.critical(info)
                    send_mail(mail_to, info)
                    sys.exit()
            else:
                logger.debug("""�����ڵ�%s�������ӽڵ���ޱ仯;��ö����ڵ���û��ƥ��%sǰ׺�Ľڵ���Ϣ""" % (
                             secondary_node_abs_path, sentinel_from_info))
    except Exception, e:
        info = "NodeReconfig fatal error. error message: %s. exception type :%s" % (str(e), type(e))
        logger.critical(info)
        send_mail(mail_to, info)
        sys.exit()

if __name__ == "__main__":
    logger.info('reconfig.py sys.argv: %s', sys.argv)
    try:
        service_type = config.get("meta", "service_type")
        if len(sys.argv) != 8:
            logger.error("sys.argv�б���Ϊ:%d. �������!" % len(sys.argv))
            sys.exit()
        else:
            logger.debug("Failover triggered by sentinel. Reconfig zookeeper info start...")
        # ����Ĳ�����ʽ����: <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
        master_name = sys.argv[1]
        role = sys.argv[2]
        state = sys.argv[3]
        from_ip = sys.argv[4]
        from_port = sys.argv[5]
        to_ip = sys.argv[6]
        to_port = sys.argv[7]
        logger.info("""reconfig. ����Ĳ���Ϊ: <master-name>:%s <role>:%s <state>:%s <from-ip>:%s <from-port>:%s <to-ip>:%s <to-port>:%s""" % (master_name, role, state, from_ip, from_port, to_ip, to_port))
        # master_name����ƥ��cluster[0-9]{4}
        if re.match("cluster[0-9]{4}$", master_name):
            if role == "leader":
                # ��ͷһ����cluster_name,����cluster8888 ��β�����Ƕ˿�+ð��
                sentinel_from_info = master_name + ":" + from_ip + ":" + from_port + ":"
                sentinel_to_info = master_name + ":" + to_ip + ":" + to_port + ":"
                logger.info("service_type:%s. �ڱ���ɫΪleader. ������zk������Ϣ,sentinel_from_info: %s sentinel_to_info: %s" % (
                                                                                    service_type,
                                                                                    sentinel_from_info,
                                                                                    sentinel_to_info))
                # ��ѯ from_ip from_port �Ƿ�����������
                # ����ǣ� ��v2 failover�߼�
                # ������ǣ���Ĭ��NodeReconfig�߼�
                ret = is_version_2(from_ip, from_port)
                if ret == R_NOT_OK: #���ϵ�failover�߼�
                    NodeReconfig("/" + service_type, sentinel_from_info, sentinel_to_info)
                elif ret == R_ERROR:
                    err_msg = '����is_version_2()���ش�������!'
                    logger.error(err_msg)
                    send_mail(mail_to, err_msg)
                elif ret == R_OK:
                    v2_reconfig_process(from_ip, from_port, to_ip, to_port)
                else:
                    logger.error('never got here!')

            else:
                logger.info("""sentinel's role is %s, not leader. Only leader can update zookeeper info. Just ignore. master_name: %s""" % (role, master_name))
        else:
            logger.critical("master_name:%s �����Ϲ涨��zookeeper��Ϣ��ʽ!�������ֻ�����ڱ������ļ����������²Ż����!")
    except Exception, e:
        info = "reconfig.py fatal error. error message: %s. exception type :%s" % (str(e), type(e))
        logger.critical(info)
        send_mail(mail_to, info)
        # ����2 ������������ ����reconfig�ű���������
        sys.exit(2)