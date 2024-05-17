<?php

namespace Brivencn\Doris;

use PDO;
use PDOException;

class Doris
{
    private $pdo = null;
    private $host;      // 主机
    private $port;      // 端口
    private $username;  // 账户
    private $password; // 密码
    private $dbname; // 数据库名
    private $table = null;    // 表名
    private $field = "*";     // 字段
    private $where = "1=1";   // 条件
    private $partition = null; // 分区名


    /**
     * 构造方法
     * */
    public function __construct($config)
    {
        $this->host = $config['host'];
        $this->port = $config['port'];
        $this->username = $config['username'];
        $this->password = $config['password'];
        $this->dbname = $config['dbname'];
        $this->connect();
    }

    /**
     * 连接PDO
     * @return void
     * */
    private function connect()
    {
        try {
            $this->pdo = new PDO("mysql:host=$this->host:$this->port;dbname=$this->dbname", $this->username, $this->password);
            // 设置PDO错误模式为异常
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            echo "Error: " . $e->getMessage();
        }
    }

    /**
     * 设置表名
     * @access public
     * @param string $table 表名
     * @return $this
     */
    public function table($table)
    {
        $this->table = $table;
        return $this;
    }

    /**
     * 查询字段
     * @param string $field 字段
     * @return $this
     * */
    public function field($field)
    {
        $this->field = $field;
        return $this;
    }

    /**
     * 查询条件AND
     * @param array $where AND条件
     * @return $this
     * */
    public function where($where)
    {
        foreach ($where as $k => $v) {
            $where[$k] = $v[0] . ' ' . $v[1] . ' "' . $v[2] . '"';
        }
        $this->where = implode(" AND ", $where);
        return $this;
    }

    /**
     * 查询条件OR
     * @param array $where OR条件
     * @return $this
     * */
    public function whereOr($where)
    {
        foreach ($where as $k => $v) {
            $where[$k] = $v[0] . ' ' . $v[1] . ' "' . $v[2] . '"';
        }
        $this->where = $this->where . " OR (" . implode(" AND ", $where) . ")";
        return $this;
    }

    /**
     * 指定分区名
     * @param string $partitionName 分区名
     * @return $this
     * */
    public function partition($partitionName)
    {
        $this->partition = $partitionName;
        return $this;
    }

    /**
     * 查询
     * step1 指定表名
     * step2 指定字段，默认是 *
     * step3 指定条件 默认是1=1
     * @return array
     * */
    public function select()
    {
        try {
            $sql = 'SELECT ' . $this->field . ' FROM ' . $this->table . ' WHERE ' . $this->where;
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute();
            // 获取查询结果
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Exception $e) {
            echo $e->getMessage();
            exit();
        }
    }


    /**
     * 分页查询
     * step1 指定表名
     * step2 指定字段，默认是 *
     * step3 指定条件 默认是1=1
     * @param int $listRows 每页数量 10
     * @return array
     */
    public function paginate($listRows = 10)
    {
        // 获取当前页数和每页显示的条目数
        $page = isset($_GET['page']) ? max(1, (int)$_GET['page']) : 1;
        $offset = ($page - 1) * $listRows;
        try {
            $sql = 'SELECT ' . $this->field . ' FROM ' . $this->table . ' WHERE ' . $this->where . ' LIMIT '. $listRows .' OFFSET '. $offset;
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute();
            // 获取查询结果
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Exception $e) {
            echo $e->getMessage();
            exit();
        }

    }

    /**
     * 添加数据
     * step1 指定表名
     * @param array $insertData 要添加的数据两层数据 [ ['field' => 'value'] ]
     * @return int 添加成功数据数量
     * */
    public function add($insertData)
    {
        try {
            $data = [];
            $valData = [];
            foreach ($insertData as $k => $v) {
                foreach ($v as $k1 => $v1) {
                    $data[$k]['key'][] = $k1;
                    $data[$k]['val'][] = $v1;
                }
                $valData[] = "('" . implode("','", $data[$k]['val']) . "')";
            }
            $insertData = implode(",", $valData);
            $sql = 'INSERT INTO ' . $this->table . ' (' . implode(",",$data[0]['key']) . ') VALUES '.$insertData;
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute();
            return  $stmt->rowCount();
        } catch (\Exception $e) {
            echo $e->getMessage();
            exit();
        }
    }

    /**
     * 修改数据
     * step1 指定表名
     * step2 指定条件
     * @param array $updateData 要修改的数据一层数据  ['field' => 'value']
     * @return int 修改成功数据数量
     * */
    public function save($updateData)
    {
        try {
            $data = [];

            foreach ($updateData as $k => $v) {
                $data[] = $k . " = '" . $v . "'";
            }
            $updateData = implode(",", $data);
            $sql = 'UPDATE ' . $this->table . ' SET '. $updateData .'  WHERE ' . $this->where;
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute();
            return  $stmt->rowCount();
        } catch (\Exception $e) {
            echo $e->getMessage();
            exit();
        }
    }


    /**
     * 删除数据
     * step1 指定表名
     * step2 指定分区名
     * step3 设置条件
     * @return int 删除成功数据数量
     * */
    public function delete()
    {
        try {
            $sql = 'DELETE FROM '. $this->table .' PARTITION (' . $this->partition . ')  WHERE ' . $this->where;
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute();
            return  $stmt->rowCount();
        } catch (\Exception $e) {
            echo $e->getMessage();
            exit();
        }
    }


    /**
     * 启动事务
     * @access public
     * @return void
     */
    public function startTrans()
    {
        $this->pdo->beginTransaction();
    }

    /**
     * 用于非自动提交状态下面的查询提交
     * @access public
     * @return void
     * @throws PDOException
     */
    public function commit()
    {
        $this->pdo->commit();
    }

    /**
     * 事务回滚
     * @access public
     * @return void
     * @throws PDOException
     */
    public function rollback()
    {
        $this->pdo->rollBack();
    }

    // 析构
    public function __destruct()
    {
        $this->pdo = null;
    }
}

