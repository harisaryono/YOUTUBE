#!/usr/bin/env python3
"""
Contoh Penggunaan YouTube Transcript Framework
"""

from partial_py.youtube_transcript import YouTubeTranscript


def example_basic():
    """Contoh penggunaan dasar"""
    print("=== CONTOH PENGGUNAAN DASAR ===\n")
    
    # URL contoh (ganti dengan URL YouTube yang diinginkan)
    url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    language = "en"  # Gunakan "id" untuk bahasa Indonesia
    
    try:
        # Inisialisasi
        yt = YouTubeTranscript(url, language)
        
        # Ambil informasi video
        info = yt.get_video_info()
        print(f"Judul: {info['title']}")
        print(f"Channel: {info['channel']}\n")
        
        # Ambil transkrip
        transcript = yt.get_transcript()
        print(f"Berhasil mengambil {len(transcript)} baris transkrip\n")
        
        # Format dengan timestamp
        formatted_timestamp = yt.format_transcript('timestamp')
        print("--- Transkrip dengan Timestamp (5 baris pertama) ---")
        print('\n'.join(formatted_timestamp.split('\n')[:5]))
        print("...\n")
        
        # Format bersih
        formatted_clean = yt.format_transcript('clean')
        print("--- Transkrip Bersih (100 karakter pertama) ---")
        print(formatted_clean[:100] + "...\n")
        
        # Buat ringkasan
        summary = yt.create_summary(max_sentences=3)
        print("--- Ringkasan ---")
        print(summary + "\n")
        
        # Simpan ke file
        yt.save_to_file(formatted_timestamp, "transcript_timestamp.txt")
        yt.save_to_file(formatted_clean, "transcript_clean.txt")
        yt.save_to_file(summary, "summary.txt")
        
        print("Semua file berhasil disimpan!")
        
    except Exception as e:
        print(f"Error: {e}")


def example_all_formats():
    """Contoh penggunaan semua format transkrip"""
    print("\n=== CONTOH SEMUA FORMAT ===\n")
    
    url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    
    try:
        yt = YouTubeTranscript(url, 'en')
        yt.get_transcript()
        
        # Format timestamp
        timestamp_format = yt.format_transcript('timestamp')
        print("--- Format Timestamp ---")
        print(timestamp_format[:200] + "...\n")
        
        # Format clean
        yt.transcript = None  # Reset untuk format lain
        yt.get_transcript()
        clean_format = yt.format_transcript('clean')
        print("--- Format Clean ---")
        print(clean_format[:150] + "...\n")
        
        # Format JSON
        yt.transcript = None
        yt.get_transcript()
        json_format = yt.format_transcript('json')
        print("--- Format JSON (100 karakter) ---")
        print(json_format[:100] + "...\n")
        
        # Format SRT
        yt.transcript = None
        yt.get_transcript()
        srt_format = yt.format_transcript('srt')
        print("--- Format SRT (5 baris pertama) ---")
        print('\n'.join(srt_format.split('\n')[:5]))
        print("...\n")
        
    except Exception as e:
        print(f"Error: {e}")


def example_full_report():
    """Contoh penggunaan untuk mendapatkan laporan lengkap"""
    print("\n=== CONTOH LAPORAN LENGKAP ===\n")
    
    url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    
    try:
        yt = YouTubeTranscript(url, 'en')
        report = yt.get_full_report('timestamp')
        
        print("--- Informasi Video ---")
        for key, value in report['video_info'].items():
            print(f"{key}: {value}")
        
        print(f"\n--- Statistik Transkrip ---")
        print(f"Total durasi: {report['total_duration']:.2f} detik")
        print(f"Jumlah kata: {report['word_count']:,}")
        
        print(f"\n--- Ringkasan ---")
        print(report['summary'])
        
        print(f"\n--- Transkrip (200 karakter pertama) ---")
        print(report['transcript'][:200] + "...")
        
    except Exception as e:
        print(f"Error: {e}")


def example_indonesian_video():
    """Contoh untuk video bahasa Indonesia"""
    print("\n=== CONTOH VIDEO BAHASA INDONESIA ===\n")
    
    # URL video bahasa Indonesia (contoh)
    url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"  # Ganti dengan URL video Indonesia
    
    try:
        yt = YouTubeTranscript(url, 'id')  # 'id' untuk bahasa Indonesia
        
        info = yt.get_video_info()
        print(f"Judul: {info['title']}")
        print(f"Channel: {info['channel']}\n")
        
        transcript = yt.get_transcript()
        print(f"Berhasil mengambil {len(transcript)} baris transkrip\n")
        
        summary = yt.create_summary(max_sentences=5)
        print("--- Ringkasan ---")
        print(summary + "\n")
        
        formatted = yt.format_transcript('timestamp')
        yt.save_to_file(formatted, "transcript_indo.txt")
        yt.save_to_file(summary, "summary_indo.txt")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    print("YouTube Transcript Framework - Contoh Penggunaan")
    print("=" * 50)
    
    # Jalankan contoh-contoh
    example_basic()
    example_all_formats()
    example_full_report()
    example_indonesian_video()
