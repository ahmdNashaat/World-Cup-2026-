-- ينشئ database ومستخدم مخصص لـ Airflow metadata
-- منفصل تمامًا عن worldcup_db (بيانات المشروع الفعلية)
-- عشان لا يحصل تداخل بين الـ metadata بتاعة Airflow وبيانات الـ pipeline.

CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
CREATE DATABASE airflow_db OWNER airflow_user;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;