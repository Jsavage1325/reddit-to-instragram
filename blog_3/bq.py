from google.cloud import bigquery
import pandas as pd
import config

class BigQuery():
    """
    Class containing custom interactions with GBQ API
    """
    def __init__(self):
        dataset_name = 'post_data'
        self.client = bigquery.Client(credentials=config.get_gbq_credentials(), project=config.get_gbq_credentials().project_id,)
        self.post_table_id = f'{config.get_gbq_credentials().project_id}.{dataset_name}.posts'
        self.tmp_post_table_id = f'{config.get_gbq_credentials().project_id}.{dataset_name}.posts_temp'

    def overwrite_posts_temp_with_dataframe(self, dataframe: pd.DataFrame):
        """
        Overwrites the tempoary posts table with a pandas dataframe
        """
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[
                bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("approved", bigquery.enums.SqlTypeNames.BOOLEAN),
                bigquery.SchemaField("audio_url", bigquery.enums.SqlTypeNames.STRING),
            ],
            # Set write disposition to WRITE_TRUNCATE
            write_disposition="WRITE_TRUNCATE",
        )

        # Overwrite the table with the Dataframe
        job = self.client.load_table_from_dataframe(
            dataframe, self.tmp_post_table_id, job_config=job_config
        )  

        # Return result of job
        return job.result()  

    def update_posts_from_temp_table(self):
        """
        Runs a merge operation on the posts table using data which has been stored in the temp table
        Uses the url as a key
        """
        query = f"""
        MERGE INTO {self.post_table_id} AS posts
        USING
        (SELECT * FROM {self.tmp_post_table_id}) AS new_posts
        ON
        posts.url = new_posts.url
        WHEN MATCHED
        THEN UPDATE SET 
        posts.score = new_posts.score, posts.approved = new_posts.approved, posts.added = new_posts.added, posts.audio_url = new_posts.audio_url
        WHEN NOT MATCHED
        THEN INSERT ROW
        """
        return self.client.query(query).result()

    def get_posts_table_as_dataframe(self):
        """
        Gets the posts table as a dataframe
        """
        query = f"""
        SELECT * FROM {self.post_table_id}
        """
        return self.client.query(query).result().to_dataframe()

    def get_single_approved_image_post_as_df(self):
        """
        Gets a single approved image post as a dataframe from the posts table
        """
        query = f"""
        SELECT * FROM {self.post_table_id}
        WHERE
        approved IS TRUE
        AND added IS FALSE
        AND type = 'image'
        LIMIT 1
        """
        return self.client.query(query).result().to_dataframe()

    def get_unapproved_image_posts_as_df(self):
        """
        Gets the unapproved posts as a dataframe
        """
        query = f"""
        SELECT * FROM {self.post_table_id}
        WHERE
        approved IS NULL
        AND type = 'image'
        """
        return self.client.query(query).result().to_dataframe()

    def update_posts_using_df(self, new_posts: pd.DataFrame):
        """
        Pushed a dataframe of posts to the temp table and then uses it to update the main posts table
        """
        self.overwrite_posts_temp_with_dataframe(new_posts)
        self.update_posts_from_temp_table()