
import os
import psycopg2
import datetime
import logging
import requests

def main(mytimer):
    try:
        conn = psycopg2.connect(
            dbname=os.environ['SUPABASE_DB'],
            user=os.environ['SUPABASE_USER'],
            password=os.environ['SUPABASE_PASS'],
            host=os.environ['SUPABASE_HOST'],
            port='5432'
        )
        cursor = conn.cursor()

        # Check failures in the past 15 minutes
        cursor.execute("""
            SELECT entity_type, stage, COUNT(*) as errors
            FROM doorloop_pipeline_audit
            WHERE status = 'error'
              AND created_at > now() - interval '15 minutes'
            GROUP BY entity_type, stage
            HAVING COUNT(*) > 0
        """)
        rows = cursor.fetchall()
        if rows:
            message = "**🚨 SYNC ERROR DETECTED**\n\n"
            for row in rows:
                message += f"- {row[0]} → {row[1]}: {row[2]} errors\n"
            send_alert(message)

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error("Error in health alert function: " + str(e))

def send_alert(message):
    webhook_url = os.environ['TEAMS_WEBHOOK']
    payload = {
        "text": message
    }
    requests.post(webhook_url, json=payload)
