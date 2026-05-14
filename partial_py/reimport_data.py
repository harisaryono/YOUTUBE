import sqlite3
import csv
import os

def import_csv(conn, csv_path, table_name, column_mapping):
    print(f"Importing {csv_path} into {table_name}...")
    cursor = conn.cursor()
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Prepare INSERT statement
        cols = list(column_mapping.keys())
        placeholders = ', '.join(['?'] * len(cols))
        query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"
        
        batch_size = 1000
        batch = []
        count = 0
        
        for row in reader:
            data = []
            for col in cols:
                csv_col = column_mapping[col]
                data.append(row.get(csv_col))
            batch.append(data)
            
            if len(batch) >= batch_size:
                cursor.executemany(query, batch)
                conn.commit()
                count += len(batch)
                print(f"Processed {count} rows...")
                batch = []
        
        if batch:
            cursor.executemany(query, batch)
            conn.commit()
            count += len(batch)
            print(f"Processed {count} rows...")
            
    print(f"Done importing {table_name}. Total: {count} rows.")

def main():
    db_path = "youtube_transcripts.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    
    # 1. Map Channels
    channel_mapping = {
        'id': 'id',
        'channel_id': 'channel_id',
        'channel_name': 'channel_name',
        'channel_url': 'channel_url',
        'subscriber_count': 'subscriber_count',
        'video_count': 'video_count',
        'thumbnail_url': 'thumbnail_url',
        'updated_at': 'last_updated',
        'created_at': 'created_at'
    }
    import_csv(conn, 'channels.csv', 'channels', channel_mapping)
    
    # 2. Map Videos
    video_mapping = {
        'id': 'id',
        'video_id': 'video_id',
        'channel_id': 'channel_id',
        'title': 'title',
        'description': 'description',
        'duration': 'duration',
        'upload_date': 'upload_date',
        'view_count': 'view_count',
        'like_count': 'like_count',
        'comment_count': 'comment_count',
        'video_url': 'video_url',
        'thumbnail_url': 'thumbnail_url',
        'transcript_file_path': 'transcript_file_path',
        'summary_file_path': 'summary_file_path',
        'transcript_downloaded': 'transcript_downloaded',
        'transcript_language': 'transcript_language',
        'word_count': 'word_count',
        'line_count': 'line_count',
        'is_short': 'is_short',
        'is_member_only': 'is_member_only',
        'created_at': 'created_at',
        'updated_at': 'updated_at'
    }
    import_csv(conn, 'videos.csv', 'videos', video_mapping)
    
    conn.close()

if __name__ == "__main__":
    main()
