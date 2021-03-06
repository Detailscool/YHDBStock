#!/bin/sh
#数据库连接信息
#新数据库,内网
db_host='127.0.0.1'
db_user='root'
db_pw='root'
db_name='stocks'

source ~/.bash_profile

echo "___________________________begin load search data file_________________________"

data_path="./Stocks"
cd ${data_path}
ls *|while read stock_file
do
    echo $stock_file
    ../mysql_manager.py $stock_file
    mysql --local-infile=1 -h${db_host} -u${db_user} -p${db_pw} -t ${db_name} -e "LOAD DATA local INFILE '"${stock_file}"' into table ${file_name} FIELDS TERMINATED BY '\t' (date,open,high,close,low,volume,price_change,p_change,ma5,ma10,ma20,v_ma5,v_ma10,v_ma20,turnover)"

    if [ $? -ne 0 ]
    then
        echo "put $stock_file error"
        exit 1
    fi
done

echo "finish update online table, seconds:${SECONDS}"
