#!/bin/bash
source ${0%${0##*/}}common.in example


main()
{
    old_date=`date "+%D %T"`

    if [ -n "$1" ]; then
        sdate=$1
    else
        sdate=`date "+%Y%m%d" --date="1 days ago"`
    fi

    if [ -n "$2" ]; then
        edate=$2
    else
        edate=$sdate
    fi

    while [[ "$sdate" -le "$edate" ]] ;
    do
        get_shopping_cr_log $sdate
        get_nexearch_cr_log $sdate
        merge_cr_log $sdate

        sdate=`date "+%Y%m%d" --date="$sdate 1 days "`
    done

    new_date=`date "+%D %T"`

    echo "DONE [$0 $@]"
    echo "START TIME : $old_date"
    echo "FINISH TIME : $new_date"
}


# 쇼핑 클릭 로그
get_shopping_cr_log()
{
    date=$1

	# output format
	# bcookie \t timestamp \t "shopping" \t query \t gdid(nvmid) \t url

	hadoop_input1="hdfs://c3/user/cuve/korea/refine_log/nclickscr_shopping_raw/$date/*/"
    hadoop_output="$proj_hdfs/shopping_cr_log"
	$H3 dfs -rm -r -skipTrash $hadoop_output || true
	$Y3 jar $STREAM_JAR \
	$COMPRESSOPT \
	-D mapreduce.job.queuename=small \
	-D mapred.reduce.tasks=100 \
	-D mapred.reduce.slowstart.completed.maps=0.90 \
	-D mapred.job.name="$hadoop_output" \
	-input $hadoop_input1 \
	-output $hadoop_output \
	-file $SRC/shopgift_shopping_cr_log.py \
	-mapper "shopgift_shopping_cr_log.py" \
	-reducer "cat"
}

# 통검 클릭 로그
# 쇼핑 이력이 있는 비쿠키만 수집
# TODO : 통검에서만, 쇼핑컬렉션 클릭하고 ... 쇼검은 하지 않은 사용자는.... 제외됨
get_nexearch_cr_log()
{
    date=$1

	# 당일 쇼핑 클릭 로그를 남긴 bcookie만 사용하기 위하여
    $H3 dfs -text $proj_hdfs/shopping_cr_log/part* \
        | awk -F'\t' '{ print $1; }' | uniq -c | awk '{print $2 "\t" $1; }' \
        > $DATA/bc.txt

    hadoop_input1="hdfs://c3/user/cuve/korea/naver_log/pc_all_click/$date*/*"
    hadoop_input2="hdfs://c3/user/cuve/korea/naver_log/mobile_all_click/$date*/*"

	# output format
	# bcookie \t timestamp \t "nexearch" \t query \t gdid(nvmid) \t url

    hadoop_output="$proj_hdfs/nexearch_cr_log"
	$H3 dfs -rm -r -skipTrash $hadoop_output || true
	$Y3 jar $STREAM_JAR \
	$COMPRESSOPT \
	-D mapreduce.job.queuename=small \
	-D mapred.reduce.tasks=100 \
	-D mapred.reduce.slowstart.completed.maps=0.90 \
	-D mapred.job.name="$hadoop_output" \
	-input $hadoop_input1 \
	-input $hadoop_input2 \
	-output $hadoop_output \
	-file $DATA/bc.txt \
	-file $SRC/shopgift_nexearch_cr_log_by_bc.py \
	-mapper "shopgift_nexearch_cr_log_by_bc.py -d bc.txt " \
	-reducer "cat"
}


# 쇼핑로그와 통검 로그를 합침
merge_cr_log()
{
    date=$1

    hadoop_input1="$proj_hdfs/shopping_cr_log"
    hadoop_input2="$proj_hdfs/nexearch_cr_log"
    hadoop_output="$proj_hdfs/merge_cr_log/$date"
	$H3 dfs -rm -r -skipTrash $hadoop_output || true
	$Y3 jar $STREAM_JAR \
	$REDUCEOPT_1 \
	-D mapred.job.name="$hadoop_output" \
	-D mapred.text.key.partitioner.options=-k1,1 \
	-D stream.num.map.output.key.fields=2 \
	-D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
	-D mapred.text.key.comparator.options="-k1,1 -k2,2n " \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
	-input $hadoop_input1 \
	-input $hadoop_input2 \
	-output $hadoop_output \
	-file $SRC/shopgift_merge_cr_log.py \
	-mapper "cat " \
	-reducer "shopgift_merge_cr_log.py"

}


main $@
