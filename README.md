# Offair ⚡

Offair is a music player designed specifically for low-end connexion, optimizing listening over buffering time.

It is using one thread to download songs as it goes. You can use youtube sessions cookies by exporting them as `.txt`, allowing more relevent recommandation.

If the next song in playing isn't finished downloading, it'll switch over an already downloaded song to have seamless audio without long pauses.

The script was written on a 70 kb/s download connecion.

- Local music vault

- Seach tab

- Radio tab with recommandation

## Setup

One single command to install depedencies: 

- `pip install yt_dlp`

Then launch youtube_music.py and enjoy.

## Configuration

In the top-right corner on the webapp, there is 4 settings:

- Download quality -> Two quality preset for speed/quality. Used minimal "Ultra-Low" during coding.

- Max song length -> Since youtube is used as source, long-format video can be listened. Since it is a music player and not a podcast player, the hard limit is 10 minutes long, but personalizable.

- Target song length -> When radio auto-fill is enabled, songs are ranked based on length and recommandation position.

- cookies.txt filepath -> Optional, this allow the radio to use existing cookies as base to recommand songs. It will not refine your youtube algorith and is read-only.
