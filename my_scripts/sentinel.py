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
R_NOT_OK = 1 #NOT OK 并不是错误


def get_domain_name(ip):
    """
    取ip的域名，如果发生错误，就返回ip
    """
    try:
        domain_name = socket.gethostbyaddr(ip)[0]
        return domain_name.replace('.qunar.com', '')
    except:
        return ip

def make_config_server_connection():
    """
    建立配置中心的数据库连接
    注意autocommit是False
    如果有更新，插入操作，记得显示commit或rollback
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
            logger.error('获取配置中心数据库连接失败, err: %s', str(e))
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
    判断from_ip, from_port redis实例，是否在配置中心里
    如果在，则是v2版本
    如果不在，则是老版本
    注意输入参数，都是string
    port在必要时需要转成int
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
        logger.error('操作数据库时，发生异常 err: %s', str(e))
        return R_ERROR
    finally:
        close_db_cursor_and_connection(cur, conn)

def update_namespace_zk_version(ns, msg=None):
    """
    更新zk上/redis/{ns}版本
    让zk通知客户端,加载最新配置
    """
    zkc = None
    try:
        zkc = zkclient.ZkClient(zk_server_list)
        zpath = '/redis/{0}'.format(ns)
        set_ret = zkc.Set(zpath, msg, -1)
        if set_ret == zookeeper.OK:
            ok_msg = (
'namespace={0}, 更新zk成功'
            ).format(ns)
            send_mail(mail_to, ok_msg)
            return R_OK
        else:
            err_msg = (
'namespace={0}, 更新zk失败，返回值={1}'
            ).format(ns, set_ret)
            send_mail(mail_to, err_msg)
            return R_ERROR
    except Exception as e:
        ext_msg = (
'namespace={0}, 更新zk异常, err:{1}'
        ).format(ns, str(e))
        logger.error(ext_msg)
        send_mail(mail_to, ext_msg)
        return R_ERROR

def v2_reconfig_process(from_ip, from_port, to_ip, to_port):
    """
    注意输入参数，都是string
    port在必要时需要转成int
    """
    conn = make_config_server_connection()
    if not conn:
        return R_ERROR
    # 取到from_ip, from_port实例信息
    cur = conn.cursor()
    try:
        select_sql = (
'SELECT id,namespace,virtualname,cluster_ip,cluster_port,update_time'
' FROM redis_resource'
' WHERE cluster_ip=%s'
' AND cluster_port=%s'
        )
        cur.execute(select_sql, (from_ip, int(from_port),))
        if not cur.with_rows: #没有找到记录，报警
            err_msg = (
'v2_reconfig_process() 数据库中没有找到ip={ip}, port={port}'
'的redis实例信息，这种情况很危险，请DBA马上查看!'
            ).format(ip=from_ip,port=int(from_port))
            logger.error(err_msg)
            send_mail(mail_to, err_msg)
            return R_ERROR
        else:
            # 正常情况下，只会取到一条
            # 更新此实例信息
            rows = cur.fetchall()
            for cid,ns,vname,cip,cport,utime in rows:
                update_sql = (
'UPDATE redis_resource'
' SET cluster_ip=%s,cluster_port=%s'
' WHERE id=%s AND update_time=%s'
                )
                cur.execute(update_sql, (to_ip, int(to_port), cid, utime,))
                if cur.rowcount == 1: #成功
                    ok_msg = (
'v2_reconfig_process() 切换成功.'
' {cname} {oip}:{oport}@{ns} 切换到'
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
                    #更新zk
                    update_namespace_zk_version(ns, ok_msg)
                    return R_OK
                else: #失败
                    err_msg = (
'v2_reconfig_process() 更新数据库失败，这种情况很危险，请DBA马上查看!'
'from_ip={0}, from_port={1},'
'to_ip={2}, to_port={3}'
                    ).format(from_ip, from_port, to_ip, to_port)
                    conn.rollback()
                    logger.error(err_msg)
                    send_mail(mail_to, err_msg)
                    return R_ERROR
    except mysql.connector.Error as e:
        err_msg = 'v2_reconfig_process() 操作数据库时，发生异常 err: %s, 请查看！', str(e)
        logger.error(err_msg)
        send_mail(mail_to, err_msg)
        return R_ERROR
    finally:
        close_db_cursor_and_connection(cur, conn)

def NodeReconfig(first_layer_node, sentinel_from_info, sentinel_to_info):
    zkc = zkclient.ZkClient(zk_server_list)
    if first_layer_node not in ("/redis", "/mysql"):
        logger.critical("第一层节点必须是/redis或/mysql")
        sys.exit()
    second_layer_nodes = zkc.GetChildren(first_layer_node, None)
    logger.debug("从一级节点%s获取的所有二级节点列表:%s" % (first_layer_node, str(second_layer_nodes)))
    try:
        for second_layer_node in second_layer_nodes:
            # 标记这个secondary_node是否需要被更新.要被更新意味着客户端的连接池需要重建
            secondary_node_need_update = 0
            third_layer_nodes = zkc.GetChildren(first_layer_node + "/" + second_layer_node, None)
            logger.debug("从二级节点%s获取的对应三级节点列表为:%s" % ((str(second_layer_node),
                                                                     str(third_layer_nodes))))
            for third_layer_node in third_layer_nodes:
                if not re.match(r'cluster(\d+):\d+\.\d+\.\d+\.\d+:\1::\d+:\d+:\d+', third_layer_node):
                    info = "节点/redis/%s/%s路径格式错误 请修复" % (second_layer_node, third_layer_node)
                    #logger.critical(info)
                    # 测试环境下，格式错误，就不发报警了，太烦
                    #send_mail(mail_to, info)

                if third_layer_node.startswith(sentinel_from_info):
                    # 标记这个二级节点需要被更新,一旦需要被更新,后续出现任何错误,都是严重的错误,都会导致zk中的信息
                    # 与sentinel看到的信息不一致,需要严格排查
                    secondary_node_need_update = 1
                    from_node = third_layer_node
                    to_node = from_node.replace(sentinel_from_info, sentinel_to_info, 1)
                    # 求出节点的绝对路径
                    from_node_abs_path = first_layer_node + "/" + second_layer_node + "/" + from_node
                    to_node_abs_path = first_layer_node + "/" + second_layer_node + "/" + to_node
                    node_data = """#Comments created by redis sentinel reconfig.py. First create node:""" \
                                """ %s, then delete node: %s""" % (to_node_abs_path, from_node_abs_path)
                    try:
                        ret_create = zkc.Create(to_node_abs_path, node_data, 0)
                        # create默认返回的是创建的路径
                        logger.info("创建节点:%s成功" % (to_node_abs_path))
                    except zookeeper.NodeExistsException, e:
                        info = "redis哨兵创建zookeeper节点%s失败. 错误信息:%s" % (to_node_abs_path, str(e))
                        logger.critical(info)
                        send_mail(mail_to, info)
                        sys.exit()
                    try:
                        ret_delete = zkc.Delete(from_node_abs_path)
                        # delete成功,则返回OK
                        if ret_delete == zookeeper.OK:
                            logger.info("删除节点:%s,操作成功. 返回值: %s" % (from_node_abs_path, str(ret_delete)))
                        if ret_delete == zookeeper.NONODE:
                            info = "删除节点:%s,操作失败.节点不存在.%s" % (from_node_abs_path, str(ret_delete))
                            logger.critical(info)
                            send_mail(mail_to, info)
                            sys.exit()
                    except Exception, e:
                        info = "删除节点:%s. 发生致命错误: %s" % (from_node_abs_path, str(e))
                        logger.critical(info)
                        send_mail(mail_to, info)
                        sys.exit()

            secondary_node_abs_path = first_layer_node + "/" + second_layer_node
            if secondary_node_need_update == 1:
                # 当第二层下的所有节点修改完毕后,开始更新第二层节点的版本号,视为提交
                logger.warn("二级节点%s的子节点已经发生变化,将更新该二级节点版本号" % secondary_node_abs_path)
                ret_set = zkc.Set(secondary_node_abs_path, "Standing on shoudlers of giants. QUNAR DBA!", -1)
                # 正常情况下,zookeeper会返回zookeeper.OK,其值为0,表示节点创建成功
                if ret_set == zookeeper.OK:
                    logger.info("更新二级节点%s版本号成功,返回值: %s" % (secondary_node_abs_path, str(ret_set)))
                    success_message = """哨兵触发主从切换,zookeeper的二级节点%s下匹配%s的节点均成功切换到%s""" % (
                                                                                              secondary_node_abs_path,
                                                                                              sentinel_from_info,
                                                                                              sentinel_to_info)
                    logger.info(success_message + " 节点%s的相关failover操作已经处理完毕" % sentinel_from_info)
                    send_mail(mail_to, success_message)
                    # 当前任何一个redis实例都只会最多处于一个二级节点之下 因为zk自身的节点查找比较困难 程序只能
                    # 顺序遍历 但是一旦找到匹配的节点 就不用再继续查找下去 因为如果存在 那么必然就是这里
                    # 某个实例出现故障后 最多只会有一个二级节点受到影响 因此程序走到这一步就可以退出了 说明
                    # 涉及该节点的变更已经完成
                    sys.exit()
                else:
                    info = "更新二级节点%s版本号失败.返回值: %s" % (secondary_node_abs_path, str(ret_set))
                    logger.critical(info)
                    send_mail(mail_to, info)
                    sys.exit()
            else:
                logger.debug("""二级节点%s的所有子节点均无变化;或该二级节点下没有匹配%s前缀的节点信息""" % (
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
            logger.error("sys.argv列表长度为:%d. 输入错误!" % len(sys.argv))
            sys.exit()
        else:
            logger.debug("Failover triggered by sentinel. Reconfig zookeeper info start...")
        # 传入的参数格式如下: <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
        master_name = sys.argv[1]
        role = sys.argv[2]
        state = sys.argv[3]
        from_ip = sys.argv[4]
        from_port = sys.argv[5]
        to_ip = sys.argv[6]
        to_port = sys.argv[7]
        logger.info("""reconfig. 传入的参数为: <master-name>:%s <role>:%s <state>:%s <from-ip>:%s <from-port>:%s <to-ip>:%s <to-port>:%s""" % (master_name, role, state, from_ip, from_port, to_ip, to_port))
        # master_name必须匹配cluster[0-9]{4}
        if re.match("cluster[0-9]{4}$", master_name):
            if role == "leader":
                # 开头一定是cluster_name,例如cluster8888 结尾必须是端口+冒号
                sentinel_from_info = master_name + ":" + from_ip + ":" + from_port + ":"
                sentinel_to_info = master_name + ":" + to_ip + ":" + to_port + ":"
                logger.info("service_type:%s. 哨兵角色为leader. 将更新zk配置信息,sentinel_from_info: %s sentinel_to_info: %s" % (
                                                                                    service_type,
                                                                                    sentinel_from_info,
                                                                                    sentinel_to_info))
                # 查询 from_ip from_port 是否在配置中心
                # 如果是， 走v2 failover逻辑
                # 如果不是，走默认NodeReconfig逻辑
                ret = is_version_2(from_ip, from_port)
                if ret == R_NOT_OK: #走老的failover逻辑
                    NodeReconfig("/" + service_type, sentinel_from_info, sentinel_to_info)
                elif ret == R_ERROR:
                    err_msg = '调用is_version_2()返回错误，请检查!'
                    logger.error(err_msg)
                    send_mail(mail_to, err_msg)
                elif ret == R_OK:
                    v2_reconfig_process(from_ip, from_port, to_ip, to_port)
                else:
                    logger.error('never got here!')

            else:
                logger.info("""sentinel's role is %s, not leader. Only leader can update zookeeper info. Just ignore. master_name: %s""" % (role, master_name))
        else:
            logger.critical("master_name:%s 不符合规定的zookeeper信息格式!这种情况只有在哨兵配置文件错误的情况下才会出现!")
    except Exception, e:
        info = "reconfig.py fatal error. error message: %s. exception type :%s" % (str(e), type(e))
        logger.critical(info)
        send_mail(mail_to, info)
        # 返回2 代表致命错误 但是reconfig脚本不会重试
        sys.exit(2)