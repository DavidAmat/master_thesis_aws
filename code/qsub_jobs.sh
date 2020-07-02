for i in {0..839}
do
	qsub jobs/job_$i.sh
done
