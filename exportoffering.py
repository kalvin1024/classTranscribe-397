#!/usr/bin/env python3

import os
import sys
import json
import requests
import urllib3
import re
from datetime import datetime

# See https://www.kaltura.com/api_v3/xsdDoc/?type=bulkUploadXml.bulkUploadXML
# Also, tip of the hat to Liam Moran for providing a working example with captions

# Course offering to download 
#courseOffering = '6cb18e07-8df2-4b44-86ab-1ef1117e8eb3' #CS374 FA202
courseOffering  = '2c7a83cc-e2f3-493a-ae65-33f9c998e8ed' # Test course  with some transcriptions

# Download options:
download_transcriptions = True
download_videos = False
download_dir = 'download'
# Filter options
regex_exclude_video_name=""
regex_include_playlist_name="^Lecture 5"
regex_exclude_playlist_name="Discussion"
include_caption_language_codes="" #e.g. en-us,ko,es
# xml output (for bulk upload into for example Kaltura)
# Bulk upload options
xml_userid='rogerrabbit' #lowercase netid of the owner
xml_entitled_edit_users='angrave,moran' # Comma separated netids of additional users who have edit access
xml_category_id=None #Channel Id/category number. Ignored if value is None e.g. 200110393
xml_filename = None # "mrss-bulkupload.xml"
xml_caption_format='srtt'

if xml_filename:
	assert(xml_userid)
	assert xml_caption_format in ['', 'vtt','srt'] # Only srt has been tested
	
# Source
ctbase='https://classtranscribe.illinois.edu'
# For testing / pulling content from other instances...
#ctbase='https://localhost'

session = requests.Session()

def expectOK(response):
	if(response.status_code != 200):
		print(f"{response.status_code}:{response.reason}")
		sys.exit(1)
	return response

# MediaSpace subtitle languages
def to_language_word(code):
	mapping = {'zh-hans' : 'Chinese', 'ko':'Korean', 'en-us': 'English', 'fr' : 'French','es':'Spanish' }
	return mapping.get(code.lower(),'?')

def getPlaylistsForCourseOffering(cid):
	url=f"{ctbase}/api/Playlists/ByOffering/{cid}"
	try:
		return  expectOK(session.get(url)).json()
		
	except:
		print(f"{url} expected json response");
		print(session.get(url).raw)
		sys.exit(1)

def getPlaylistDetails(pid):
	url=f"{ctbase}/api/Playlists/{pid}"
	return expectOK(session.get(url)).json() 

def getTranscriptionContent(path):
	return expectOK(session.get(ctbase + path)).text

def lazy_download_file(path,file, automatic_extension=True):
	if not path:
		return
	if automatic_extension and ('.' in path.split('/')[-1]):
		extension=path.split('/')[-1].split('.')[-1]
		file += '.'+ extension
	
	if os.path.exists(file):
		try:
			file_time = datetime.fromtimestamp(os.path.getmtime(file))
			file_size = os.path.getsize(file)
			r = session.head(ctbase + path)
			
			content_length =  int(r.headers['Content-Length'] )
			last_mod =  datetime.strptime(r.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z') 
			if file_size == content_length and (file_time > last_mod):
				return
		except:
			pass
    
	with session.get(ctbase + path, stream=True) as r:
		expectOK(r)
		print(f"{path} -> {file}", end=' ')
		with open(file,"wb") as fd:
			for chunk in r.iter_content(chunk_size=1<<20):
				if chunk:
					fd.write(chunk)
					sys.stdout.write('.')
					sys.stdout.flush()
		print()

# There may be characters in titles we don't want in a filename
def sanitize(name):
	return re.sub(r"[^a-zA-Z0-9,()]", "_", name.strip())
	
def get_video(path,directory,basefilename):
	file = f"{directory}/{basefilename}"
	lazy_download_file(path, file)

def get_transcriptions(transcriptions,directory,basefilename):
	if transcriptions:
		for t in transcriptions:
			filename =  f"{directory}/{basefilename}-{sanitize(t['language'])}"
			lazy_download_file(t['path'],filename)
			lazy_download_file(t['srtPath'],filename) 
	else :
		print(f"No transcriptions for {directory}/{basefilename}")


def main():
	auth=os.environ.get('CLASSTRANSCRIBE_AUTH','')
	if len(auth)<100:
		print("Login to https://classtranscribe.illinois.edu then run localstorage['authtoken'] in the dev console or use the Applications tab (Chrome) to grab the authToken")
		print("SET CLASSTRANSCRIBE_AUTH=(authToken)" if "windir" in os.environ else  "export CLASSTRANSCRIBE_AUTH=(authToken)" )
		sys.exit(1)
	auth = auth.replace('"','').replace("'",'').strip()

	session.headers.update({'Authorization':  "Bearer "+ auth})
	if  'localhost' in ctbase:
		session.verify = False
		urllib3.disable_warnings()

	video_count = 0
	transcription_count = 0
	videos_without_transcriptions = []
	playlist_count = 0
	skipped_videos = []
	skipped_playlists = []
	bulk_xml = ""

	playlists = getPlaylistsForCourseOffering(courseOffering)
	
	print(f"{len(playlists)} found: {','.join([p['name'] for p in playlists])}")

	for p in playlists:
		if regex_exclude_playlist_name and re.search(regex_exclude_playlist_name, p['name']):
			skipped_playlists.append( p['name'])
			continue
		if regex_include_playlist_name and not re.search(regex_include_playlist_name, p['name']):
			skipped_playlists.append( p['name'])
			continue

		playlist_count += 1
		print(f"\n** Playlist {playlist_count}. ({p['id']}):{p['name']}")

		details = getPlaylistDetails(p['id'])
		medias = details['medias']

		if download_videos or download_transcriptions:
			directory = f"{download_dir}/{sanitize(p['name'])}"
			os.makedirs(directory,exist_ok=True)

		for m in medias:
			if regex_exclude_video_name and re.search(regex_exclude_video_name,m['name']):
				print(" Skipping video {m['name']}")
				skipped_videos.append(f"{p['name']}/{m['name']}")
				continue

			if m['video'] is None:
				print(" Skipping video {m['name']} because no video file exists")
				skipped_videos.append(f"{p['name']}/{m['name']}")
				continue
				
			video_count += 1
			print(f" {video_count}: '{p['name']}/{m['name']}' ({m['id']})({m['video']['id']})")

			path = m['video']['video1Path']
			basename = sanitize(m['name'])
			if download_videos:
				get_video(path,directory,basename)
			if download_transcriptions:
				get_transcriptions(m['transcriptions'],directory,basename)

			# Todo should escape the CDATA contents
			bulk_xml += "\n\n<item><action>add</action><type>1</type>"
			bulk_xml += f"\n <userId>{xml_userid.lower().strip()}</userId>"

			if xml_entitled_edit_users:
				bulk_xml += "\n <entitledUsersEdit>"
				for u in xml_entitled_edit_users.split(','):
					bulk_xml += f"<user>{u.lower().strip()}</user>"
				bulk_xml += "</entitledUsersEdit>"
 				
			bulk_xml += f"\n <name><![CDATA[{m['name']}]]></name>"
			
			abstract = f"Lecture Video for {p['name']}"
			bulk_xml += f"\n <description><![CDATA[{abstract}]]></description>"

			if xml_category_id:
				bulk_xml += f"\n <categories><categoryId>{xml_category_id}</categoryId></categories>"

			mediaType = '1' #
			bulk_xml += f"\n <media><mediaType>{mediaType}</mediaType></media>"
			bulk_xml +=  f"""\n <contentAssets><content><urlContentResource url="{ctbase + path}"></urlContentResource></content></contentAssets>"""

			subtitles_xml = ""
			filter_languages = list(map(lambda x: x.lower().strip(), include_caption_language_codes.split(','))) if include_caption_language_codes else None
			default_found = False
			for t in m['transcriptions']:
				
				language = to_language_word(t['language'])
				print(f"  |--Transcription:{t['language']}:{language} at {t['path']} srt:{t['srtPath']} ({t['id']})")
				# https://www.kaltura.com/api_v3/testmeDoc/enums/KalturaCaptionType.html
					# 1=srt, 2=dxfp, 3=webvtt, 4=CAP, 5= SCC
					# but only srt and dxfp are supported by the editor (according to docs/support tickets)
				 
				caption_path = None
				format_type = None
				if xml_caption_format =='srt' :
					format_type = 1
					caption_path = t['srtPath'] 
				elif xml_caption_format =='vtt' :
					format_type = 3 # Not tested
					caption_path = t['path'] 
				if language != '?' and caption_path  and ((filter_languages is None) or (t['language'].lower() in filter_languages)):				
					is_default = False
					label=f"{language}"
					if  t['language'][0:2].lower()=='en' and not default_found:
						default_found = True
						is_default = True
						label += " (ClassTranscribe)"

					subtitles_xml +=f"""\n    <subTitle isDefault="{str(is_default).lower()}" format="{format_type}" lang="{language}" label="{label}">"""
					subtitles_xml +=f"""\n    <tags></tags><urlContentResource url="{ctbase + caption_path }"></urlContentResource>\n    </subTitle>"""
					transcription_count +=1

			if subtitles_xml:
				bulk_xml += f"\n   <subTitles>{subtitles_xml}</subTitles>"

			if not default_found:
				videos_without_transcriptions.append(f"{p['name']}/{m['name']} ({m['id']})")

			bulk_xml += "\n</item>\n"
			print()
	
	if xml_filename :
		print(f"\nWriting xml to {xml_filename}")
		header = '<?xml version="1.0" encoding="UTF-8"?>'
		header += '<mrss xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="ingestion.xsd">\n'
		with open(xml_filename,"w", encoding='utf-8') as xml_fh:
			xml_fh.write(f"{header}<channel>{bulk_xml}</channel></mrss>")

	print('\nSummary:')
	print(f"Processed {video_count} videos; {transcription_count} transcriptions; in {playlist_count} playlists.")
	print(f"No default transcriptions for {len(videos_without_transcriptions)} videos:{','.join(videos_without_transcriptions)}")
	print(f"Skipped {len(skipped_playlists)} non-matching playlists(s):{','.join(skipped_playlists)}")
	print(f"Skipped {len(skipped_videos)} non-matching video(s):{','.join(skipped_videos)}")

main()
