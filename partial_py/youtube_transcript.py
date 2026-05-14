#!/usr/bin/env python3
"""
YouTube Transcript Framework
Framework untuk mengambil transkrip YouTube, memformat, dan membuat resume
"""

import yt_dlp
import json
import re
from typing import Dict, List, Optional
from datetime import datetime


class YouTubeTranscript:
    def __init__(self, url: str, language: str = 'id'):
        """
        Inisialisasi YouTube Transcript
        
        Args:
            url: URL video YouTube
            language: Kode bahasa untuk transkrip (default: 'id' untuk Indonesia)
        """
        self.url = url
        self.language = language
        self.video_info = None
        self.transcript = None
        self.formatted_transcript = None
        self.summary = None
    
    def get_video_info(self) -> Dict:
        """
        Mengambil informasi video YouTube
        
        Returns:
            Dict berisi informasi video
        """
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
            'format': 'best',
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Gunakan extract_flat untuk mendapatkan info tanpa download
                info = ydl.extract_info(self.url, download=False, process=False)
                
                # Jika info kurang lengkap, coba lagi dengan extract_info biasa
                if not info.get('title'):
                    info = ydl.extract_info(self.url, download=False)
                
                self.video_info = {
                    'title': info.get('title', 'Unknown'),
                    'channel': info.get('channel', info.get('uploader', 'Unknown')),
                    'duration': info.get('duration', 0),
                    'upload_date': info.get('upload_date', 'Unknown'),
                    'view_count': info.get('view_count', 0),
                    'description': info.get('description', '')
                }
                return self.video_info
        except Exception as e:
            raise Exception(f"Gagal mengambil info video: {str(e)}")
    
    def get_transcript(self) -> List[Dict]:
        """
        Mengambil transkrip video YouTube
        
        Returns:
            List berisi data transkrip dengan timestamp dan teks
        """
        # Pertama, ambil info video untuk mendapatkan URL subtitle
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
            'writesubtitles': False,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(self.url, download=False, process=False)
                
                # Coba ambil subtitle manual
                subtitles = info.get('subtitles', {}).get(self.language)
                if not subtitles:
                    # Coba subtitle otomatis
                    subtitles = info.get('automatic_captions', {}).get(self.language)
                
                if not subtitles:
                    raise Exception(f"Tidak ada transkrip tersedia untuk bahasa '{self.language}'")
                
                # Download subtitle sebagai JSON dari URL langsung
                sub_url = subtitles[0]['url']
                import urllib.request
                with urllib.request.urlopen(sub_url) as response:
                    sub_data = json.loads(response.read().decode('utf-8'))
                
                # Parse transkrip
                self.transcript = []
                for event in sub_data.get('events', []):
                    if event.get('segs'):
                        text = ''.join([seg.get('utf8', '') for seg in event['segs']])
                        if text.strip():
                            self.transcript.append({
                                'start': event.get('tStartMs', 0) / 1000,
                                'duration': event.get('dDurationMs', 0) / 1000,
                                'text': text.strip()
                            })
                
                return self.transcript
                
        except Exception as e:
            raise Exception(f"Gagal mengambil transkrip: {str(e)}")
    
    def format_transcript(self, format_type: str = 'timestamp') -> str:
        """
        Memformat transkrip sesuai format yang diinginkan
        
        Args:
            format_type: Tipe format ('timestamp', 'clean', 'json', 'srt')
        
        Returns:
            String transkrip yang sudah diformat
        """
        if not self.transcript:
            raise Exception("Transkrip belum diambil. Panggil get_transcript() terlebih dahulu.")
        
        if format_type == 'timestamp':
            self.formatted_transcript = self._format_timestamp()
        elif format_type == 'clean':
            self.formatted_transcript = self._format_clean()
        elif format_type == 'json':
            self.formatted_transcript = self._format_json()
        elif format_type == 'srt':
            self.formatted_transcript = self._format_srt()
        else:
            raise ValueError(f"Format tidak dikenal: {format_type}")
        
        return self.formatted_transcript
    
    def _format_timestamp(self) -> str:
        """Format transkrip dengan timestamp"""
        formatted = []
        for item in self.transcript:
            timestamp = self._seconds_to_timestamp(item['start'])
            formatted.append(f"[{timestamp}] {item['text']}")
        return '\n'.join(formatted)
    
    def _format_clean(self) -> str:
        """Format transkrip bersih tanpa timestamp"""
        return ' '.join([item['text'] for item in self.transcript])
    
    def _format_json(self) -> str:
        """Format transkrip sebagai JSON"""
        return json.dumps(self.transcript, indent=2, ensure_ascii=False)
    
    def _format_srt(self) -> str:
        """Format transkrip sebagai SRT (SubRip)"""
        formatted = []
        for i, item in enumerate(self.transcript, 1):
            start = self._seconds_to_timestamp(item['start'])
            end_time = item['start'] + item['duration']
            end = self._seconds_to_timestamp(end_time)
            formatted.append(f"{i}\n{start} --> {end}\n{item['text']}\n")
        return '\n'.join(formatted)
    
    def _seconds_to_timestamp(self, seconds: float) -> str:
        """Konversi detik ke format timestamp HH:MM:SS.mmm"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        millis = int((seconds % 1) * 1000)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"
    
    def create_summary(self, max_sentences: int = 5) -> str:
        """
        Membuat ringkasan dari transkrip
        
        Args:
            max_sentences: Jumlah maksimal kalimat dalam ringkasan
        
        Returns:
            String ringkasan transkrip
        """
        if not self.transcript:
            raise Exception("Transkrip belum diambil. Panggil get_transcript() terlebih dahulu.")
        
        # Gabungkan semua teks
        full_text = self._format_clean()
        
        # Bersihkan teks
        sentences = re.split(r'[.!?]+', full_text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if not sentences:
            return "Tidak bisa membuat ringkasan dari transkrip kosong."
        
        # Sederhana: ambil kalimat pertama dan beberapa kalimat terakhir
        # Ini adalah metode dasar - bisa ditingkatkan dengan NLP
        intro_sentences = sentences[:2]
        middle_start = len(sentences) // 3
        middle_end = 2 * len(sentences) // 3
        conclusion_sentences = sentences[-min(3, len(sentences)):]
        
        important_sentences = intro_sentences + conclusion_sentences
        
        # Batasi jumlah kalimat
        summary_sentences = important_sentences[:max_sentences]
        
        self.summary = ' '.join(summary_sentences) + '.'
        
        return self.summary
    
    def save_to_file(self, content: str, filename: str):
        """
        Menyimpan konten ke file
        
        Args:
            content: Konten yang akan disimpan
            filename: Nama file
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Berhasil menyimpan ke: {filename}")
        except Exception as e:
            raise Exception(f"Gagal menyimpan file: {str(e)}")
    
    def get_full_report(self, format_type: str = 'timestamp') -> Dict:
        """
        Mendapatkan laporan lengkap berupa info video, transkrip, dan ringkasan
        
        Args:
            format_type: Tipe format untuk transkrip
        
        Returns:
            Dict berisi laporan lengkap
        """
        if not self.video_info:
            self.get_video_info()
        
        if not self.transcript:
            self.get_transcript()
        
        if not self.formatted_transcript:
            self.format_transcript(format_type)
        
        if not self.summary:
            self.create_summary()
        
        return {
            'video_info': self.video_info,
            'transcript': self.formatted_transcript,
            'summary': self.summary,
            'total_duration': sum([item['duration'] for item in self.transcript]),
            'word_count': len(self._format_clean().split())
        }


def main():
    """Contoh penggunaan"""
    import sys
    
    if len(sys.argv) < 2:
        print("Penggunaan: python youtube_transcript.py <URL_YOUTUBE> [bahasa]")
        print("Contoh: python youtube_transcript.py https://youtube.com/watch?v=VIDEO_ID id")
        sys.exit(1)
    
    url = sys.argv[1]
    language = sys.argv[2] if len(sys.argv) > 2 else 'id'
    
    try:
        # Inisialisasi
        print(f"Mengambil transkrip dari: {url}")
        yt = YouTubeTranscript(url, language)
        
        # Ambil info video
        print("\n=== INFO VIDEO ===")
        info = yt.get_video_info()
        print(f"Judul: {info['title']}")
        print(f"Channel: {info['channel']}")
        print(f"Durasi: {info['duration']} detik")
        print(f"Views: {info['view_count']:,}")
        
        # Ambil transkrip
        print("\n=== MENGAMBIL TRANSKRIP ===")
        transcript = yt.get_transcript()
        print(f"Berhasil mengambil {len(transcript)} baris transkrip")
        
        # Format transkrip
        print("\n=== TRANSKRIP DENGAN TIMESTAMP ===")
        formatted = yt.format_transcript('timestamp')
        print(formatted[:500] + "..." if len(formatted) > 500 else formatted)
        
        # Buat ringkasan
        print("\n=== RINGKASAN ===")
        summary = yt.create_summary()
        print(summary)
        
        # Simpan ke file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        yt.save_to_file(formatted, f"transcript_{timestamp}.txt")
        yt.save_to_file(summary, f"summary_{timestamp}.txt")
        
        print("\n=== SELESAI ===")
        print(f"Total durasi transkrip: {sum([t['duration'] for t in transcript]):.2f} detik")
        print(f"Jumlah kata: {len(yt._format_clean().split())} kata")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
