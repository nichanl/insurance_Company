#Find those Subscribers having age less than 30 and they subscribe any subgroup
import spark

df = spark.read.options(header = 'True',inferSchema = 'True', delimiter= ',').csv("s3://merocapstonebucket/input-data/subgroup/")
cf = spark.read.options(header = 'True',inferSchema = 'True', delimiter= ',').csv("s3://merocapstonebucket/input-data/subscriber/")
df.createOrReplaceTempView("subgroup")
cf.createOrReplaceTempView("subs")
t = spark.sql("select subs.first_name, subs.last_name , subs.Birth_date from subs inner join subgroup ON subs.Subgrp_id = subgroup.Subgrp_id and datediff(current_date(), cast(subs.Birth_date as date)) / 365.25 < 30")
t.show()
t.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.590183840691.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "capstone.req1").option("aws_iam_role", "arn:aws:iam::590183840691:role/redshiftadmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://chankogluebucket/temp-dir/").option("user", "admin").option("password", "Seventeen17$").save()

#Find out which group has maximum subgroups.
df = spark.read.option('header','True').csv("s3://merocapstonebucket/input-data/grpsubgrp/grpsubgrp.csv")

df.createOrReplaceTempView("subs")

cef = spark.sql("select SubGrp_ID, count(*) as Group_count from subs group by SubGrp_Id order by Group_count desc limit 1 ")
cef.show()
cef.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.590183840691.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "capstone.maxsubgrp").option("aws_iam_role", "arn:aws:iam::590183840691:role/redshiftadmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://chankogluebucket/temp-dir/").option("user", "admin").option("password", "Seventeen17$").save()

#Find out hospital which serve most number of patients
df = spark.read.option('header','True').csv("s3://merocapstonebucket/input-data/patient_records/Patient_records.csv")
cf = spark.read.option('header','True').csv("s3://merocapstonebucket/input-data/hospital/hospital.csv")

df.createOrReplaceTempView("patient")
cf.createOrReplaceTempView("hospital")
cef = spark.sql("select h.Hospital_name, count(h.Hospital_name) as hos_count from hospital h inner join patient p ON p.Hospital_id = h.Hospital_id group by h.Hospital_name order by hos_count desc LIMIT 1")
cef.show()
cef.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.590183840691.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "capstone.popularhospital").option("aws_iam_role", "arn:aws:iam::590183840691:role/redshiftadmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://chankogluebucket/temp-dir/").option("user", "admin").option("password", "Seventeen17$").save()

#Which disease has a maximum number of claims.
df = spark.read.option('header','True').json("s3://merocapstonebucket/input-data/claims/claims.json")
df.createOrReplaceTempView("claim")
cef = spark.sql("select disease_name, count(disease_name) as dis_count from claim where claim_id is not NUll group by disease_name order by dis_count desc limit 1")
cef.show()
cef.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.590183840691.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "capstone.disease_claims").option("aws_iam_role", "arn:aws:iam::590183840691:role/redshiftadmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://chankogluebucket/temp-dir/").option("user", "admin").option("password", "Seventeen17$").save()

#Find out total number of claims which were rejected
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAYS2NSXOZT6RCZCUF")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df = spark.read.json("s3://merocapstonebucket/input-data/claims/claims.json")
df.show()
df2 = df.filter("Claim_or_Rejected == 'Y'")
df3 = df2.count()
print("The total numner of claims which were rejected were", df3,".")

df2.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.590183840691.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "capstone.rejected_claims").option("aws_iam_role", "arn:aws:iam::590183840691:role/redshiftadmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://chankogluebucket/temp-dir/").option("user", "admin").option("password", "Seventeen17$").save()

#From where most claims are coming (city)
cf = spark.read.option('header', 'True').csv("s3://merocapstonebucket/input-data/patient_records/Patient_records.csv")
cf.show()

cf2 = cf.groupBy("city").count()
cf3 = cf2.orderBy("count", ascending=False)
first = cf3.first()
print(first)

cf3.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.590183840691.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "capstone.mostcity_claims").option("aws_iam_role", "arn:aws:iam::590183840691:role/redshiftadmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://chankogluebucket/temp-dir/").option("user", "admin").option("password", "Seventeen17$").save()

#Which groups of policies subscriber subscribe mostly Government or private
df = spark.read.option('header','True').csv("s3://merocapstonebucket/input-data/group/group.csv")
df.createOrReplaceTempView("govtopri")
cef = spark.sql("select * from govtopri where Grp_Type like 'Gov%' order by Grp_Name")
cef.show()
cef.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.590183840691.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "capstone.govtorpri").option("aws_iam_role", "arn:aws:iam::590183840691:role/redshiftadmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://chankogluebucket/temp-dir/").option("user", "admin").option("password", "Seventeen17$").save()

#Average monthly premium subscriber pay to insurance company.
df = spark.read.option('header','True').csv("s3://merocapstonebucket/input-data/subgroup/subgroup.csv")
df.createOrReplaceTempView("subg")
cef = spark.sql("select Avg(Monthly_Premium) as Monthly_Premium from subg")
cef.show()
cef.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.590183840691.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "capstone.monthlyprem").option("aws_iam_role", "arn:aws:iam::590183840691:role/redshiftadmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://chankogluebucket/temp-dir/").option("user", "admin").option("password", "Seventeen17$").save()

#List all the patients below age of 18 who admit for cancer

df = spark.read.option('header','True').csv("s3://merocapstonebucket/input-data/patient_records/Patient_records.csv")
df.createOrReplaceTempView("patient_record")
cef = spark.sql("SELECT * FROM patient_record WHERE YEAR('2024-01-20') - YEAR(patient_birth_date) >= 18 AND disease_name LIKE '% cancer' ")
cef.show()



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
