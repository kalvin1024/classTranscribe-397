#!/usr/bin/env python3

import os
import sys
import json
import requests
import urllib3
import re
from datetime import datetime
from copy import deepcopy
import pprint
# See https://www.kaltura.com/api_v3/xsdDoc/?type=bulkUploadXml.bulkUploadXML
# Also, tip of the hat to Liam Moran for providing a working example with captions

# Course offering to download 
# batch grab anything here for crawling all classes
# courseOffering = '6cb18e07-8df2-4b44-86ab-1ef1117e8eb3' #CS374 FA202
# courseOffering  = '2c7a83cc-e2f3-493a-ae65-33f9c998e8ed' # Test course  with some transcriptions

# Download options:
download_transcriptions = True
download_videos = False
download_dir = 'download'
# Filter options
regex_exclude_video_name=""
regex_include_playlist_name="^Lecture" # TODO: Second Pull Disable This Matching Pattern to Upload More
regex_exclude_playlist_name="Discussion"
include_caption_language_codes="en-us" #e.g. en-us,ko,es

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
ctbase='https://classtranscribe.illinois.edu' # prod server
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
		return None
		# sys.exit(1)

def getPlaylistDetails(pid):
	url=f"{ctbase}/api/Playlists/{pid}"
	return expectOK(session.get(url)).json() 
	

def getTranscriptionContent(path):
	return expectOK(session.get(ctbase + path)).text

def get_all_offerings():
	url=f"{ctbase}/api/CourseOfferings"
	course_responses = expectOK(session.get(url)).json()
	rets = {}
	for course_response in course_responses:
		offering_contents = course_response['offerings']
		for offering in offering_contents:
			if offering:
				course_offering_item = {
					'offeringId': offering['id'],
					'offeringCourseName': offering['courseName'].replace(':', ''),
					'offeringSectionName': offering['sectionName'].replace('/', ' '),
					'description': offering['description']
				}
				# print(course_offering_item)
				rets[offering['id']] = course_offering_item
		else:
			print(f"null entry retrieved from API")
		# rets.extend(ret)
	return rets
	
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

def get_instructor_infos(offering_dict):
	instructor_infos = {}
	count = 0
	for offering_key in offering_dict:
		url=f"{ctbase}/api/Offerings/{offering_key}"
		offering = offering_dict[offering_key]
		print(f"offering course is: {offering['offeringCourseName']}-{offering['offeringSectionName']}")
		response = expectOK(session.get(url)).json()
		ret = {
			'offeringId': offering['offeringId'],
			'courseName': response['offering']['courseName'],
			'sectionName': response['offering']['sectionName'],
		}
		instructors = response['instructorIds']
		simp_insts = []
		for instructor in instructors:
			simp_inst = {
				'instructorId': instructor['id'],
				'university': instructor['university'],
				'firstName': instructor['firstName'],
				'lastName': instructor['lastName']
			}
			simp_insts.append(simp_inst)
		ret['instructors'] = simp_insts
		instructor_infos[offering['offeringId']] = ret # indexing it easily
		count+=1
	return instructor_infos
		
# only select transcript files on filter_languages
def get_transcriptions(transcriptions, directory, basefilename, filter_languages):
	print(f"Current transcriptions: {transcriptions}")
	if transcriptions:
		for t in transcriptions:
			filename =  f"{directory}/{basefilename}-{sanitize(t['language'])}"
			print(f"All transcriptions on filename: {filename} : {t}")
			print(f"lower transform: {t['language'].lower()}")
			if filter_languages and t['language'].lower() in filter_languages:
				lazy_download_file(t['path'], filename)
				lazy_download_file(t['srtPath'], filename) 
			elif filter_languages is None:
				lazy_download_file(t['path'], filename)
				lazy_download_file(t['srtPath'], filename) 
	else :
		print(f"No transcriptions for {directory}/{basefilename}")

def get_relevant_data(offerings):
	datalist = deepcopy(offerings)
	filter_languages = list(map(lambda x: x.lower().strip(), include_caption_language_codes.split(','))) if include_caption_language_codes else None
	print(filter_languages)
	for oid in offerings:
		playlistDetails = getPlaylistsForCourseOffering(oid)
		if playlistDetails is None:
			print(f"offering id {oid} doesn't have valid playlist")
			continue

		datalist[oid]['playlists'] = []
		for playlist in playlistDetails:
			filtered_data = {
				"playlistId": playlist['id'],
				"playlistName": sanitize(playlist['name']),
				"media": []
			}
			
			details = getPlaylistDetails(playlist['id']) # search overs the playlist id got from 
			medias = details['medias']
			
			if medias is None:
				print(f"playlist {sanitize(playlist['name'])} doesn't have valid media")
				continue

			for media in medias:
				if media:
					media_data = {
						"mediaId": media['id'],
						"mediaName": sanitize(media['name']),
						"video": media['video'],
						"transcripts": [],
						"duration": media['duration']
					}	
					if media['transcriptions'] is None:
						print(f"media {sanitize(media['name'])} doesn't have valid transcriptions")
						continue

					for t in media['transcriptions']:
						if t['language'].lower() in filter_languages:
							transcript_data = {
								"filename":  f"{sanitize(media['name'])}-{sanitize(t['language'])}",
								"transcriptId": t['id'],
								"vttPath": t['path'],
								"srtPath": t['srtPath']
							}
							media_data['transcripts'].append(transcript_data)
							# print(transcript_data)
					filtered_data['media'].append(media_data)
     
			datalist[oid]['playlists'].append(filtered_data)

	return datalist
		
		
def pull_offering_transcriptions(courseOffering):
	video_count = 0
	transcription_count = 0
	videos_without_transcriptions = []
	playlist_count = 0
	skipped_videos = []
	skipped_playlists = []
	bulk_xml = ""

	playlists = getPlaylistsForCourseOffering(courseOffering['offeringId'])
	if playlists:
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

			details = getPlaylistDetails(p['id']) # search overs the playlist id got from 
			medias = details['medias']

			if download_videos or download_transcriptions:
				# this offeringCourseName or offeringSectionName from api/CourseOfferings is having slight difference than 
				# what we get from {ctbase}/api/Offerings/{offeringId}, cannot be indexed together, need unique identifier offeringId
				# also the illegal : for folder name and / extra splitting the folders
				courseName = courseOffering['offeringCourseName'].replace(':', '')
				sectionName = courseOffering['offeringSectionName'].replace('/', ' ')
				directory = f"{download_dir}/{courseName}/{sectionName}/{sanitize(p['name'])}"
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
				filter_languages = list(map(lambda x: x.lower().strip(), include_caption_language_codes.split(','))) if include_caption_language_codes else None
				print(f"FILTERED LANGUAGES ON CONFIGURATION {include_caption_language_codes}: {filter_languages}")
				if download_videos:
					get_video(path,directory,basename)
				if download_transcriptions:
					get_transcriptions(m['transcriptions'], directory, basename, filter_languages)

		# 		# Todo should escape the CDATA contents
		# 		bulk_xml += "\n\n<item><action>add</action><type>1</type>"
		# 		bulk_xml += f"\n <userId>{xml_userid.lower().strip()}</userId>"

		# 		if xml_entitled_edit_users:
		# 			bulk_xml += "\n <entitledUsersEdit>"
		# 			for u in xml_entitled_edit_users.split(','):
		# 				bulk_xml += f"<user>{u.lower().strip()}</user>"
		# 			bulk_xml += "</entitledUsersEdit>"
					
		# 		bulk_xml += f"\n <name><![CDATA[{m['name']}]]></name>"
				
		# 		abstract = f"Lecture Video for {p['name']}"
		# 		bulk_xml += f"\n <description><![CDATA[{abstract}]]></description>"

		# 		if xml_category_id:
		# 			bulk_xml += f"\n <categories><categoryId>{xml_category_id}</categoryId></categories>"

		# 		mediaType = '1' #
		# 		bulk_xml += f"\n <media><mediaType>{mediaType}</mediaType></media>"
		# 		bulk_xml +=  f"""\n <contentAssets><content><urlContentResource url="{ctbase + path}"></urlContentResource></content></contentAssets>"""

		# 		subtitles_xml = ""
				
		# 		default_found = False
		# 		for t in m['transcriptions']:
					
		# 			language = to_language_word(t['language'])
		# 			print(f"  |--Transcription:{t['language']}:{language} at {t['path']} srt:{t['srtPath']} ({t['id']})")
		# 			# https://www.kaltura.com/api_v3/testmeDoc/enums/KalturaCaptionType.html
		# 				# 1=srt, 2=dxfp, 3=webvtt, 4=CAP, 5= SCC
		# 				# but only srt and dxfp are supported by the editor (according to docs/support tickets)
					
		# 			caption_path = None
		# 			format_type = None
		# 			if xml_caption_format =='srt' :
		# 				format_type = 1
		# 				caption_path = t['srtPath'] 
		# 			elif xml_caption_format =='vtt' :
		# 				format_type = 3 # Not tested
		# 				caption_path = t['path'] 
		# 			if language != '?' and caption_path  and ((filter_languages is None) or (t['language'].lower() in filter_languages)):				
		# 				is_default = False
		# 				label=f"{language}"
		# 				if  t['language'][0:2].lower()=='en' and not default_found:
		# 					default_found = True
		# 					is_default = True
		# 					label += " (ClassTranscribe)"

		# 				subtitles_xml +=f"""\n    <subTitle isDefault="{str(is_default).lower()}" format="{format_type}" lang="{language}" label="{label}">"""
		# 				subtitles_xml +=f"""\n    <tags></tags><urlContentResource url="{ctbase + caption_path }"></urlContentResource>\n    </subTitle>"""
		# 				transcription_count +=1

		# 		if subtitles_xml:
		# 			bulk_xml += f"\n   <subTitles>{subtitles_xml}</subTitles>"

		# 		if not default_found:
		# 			videos_without_transcriptions.append(f"{p['name']}/{m['name']} ({m['id']})")

		# 		bulk_xml += "\n</item>\n"
		# 		print()
		
		# if xml_filename :
		# 	print(f"\nWriting xml to {xml_filename}")
		# 	header = '<?xml version="1.0" encoding="UTF-8"?>'
		# 	header += '<mrss xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="ingestion.xsd">\n'
		# 	with open(xml_filename,"w", encoding='utf-8') as xml_fh:
		# 		xml_fh.write(f"{header}<channel>{bulk_xml}</channel></mrss>")
	
		print('\nSummary:')
		print(f"Processed {video_count} videos; {transcription_count} transcriptions; in {playlist_count} playlists.")
		print(f"No default transcriptions for {len(videos_without_transcriptions)} videos:{','.join(videos_without_transcriptions)}")
		print(f"Skipped {len(skipped_playlists)} non-matching playlists(s):{','.join(skipped_playlists)}")
		print(f"Skipped {len(skipped_videos)} non-matching video(s):{','.join(skipped_videos)}")
	else:
		print(f"\nGiven offering ID {courseOffering['offeringId']} doesn't have a playlist")


def write_infos(info_dict, outfile):
	print(f"In total of {len(info_dict)} offerings getting supplementary info")
	with open(outfile, 'w') as f:
		json.dump(info_dict, f)
  
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

	
	offerings = get_all_offerings()
	
	pp = pprint.PrettyPrinter(indent=4)
	pp.pprint(offerings)
	print(f"pulled over {len(offerings)} course offerings in total")

	
	instructor_info_dict = get_instructor_infos(offerings)
	for offering_id in instructor_info_dict:
		if instructor_info_dict[offering_id]['courseName'] == offerings[offering_id]['offeringCourseName'] and instructor_info_dict[offering_id]['sectionName'] == offerings[offering_id]['offeringSectionName']:
			print(f"offering id {offering_id} has two tables course information matches: {instructor_info_dict[offering_id]['courseName']} / {instructor_info_dict[offering_id]['sectionName']}")
		else:
			print(f"""offering id {offering_id} has two tables course information not matching: \n 
		in CourseOffering API: {offerings[offering_id]['offeringCourseName']} / {offerings[offering_id]['offeringSectionName']} \n
		in Instructor API: {instructor_info_dict[offering_id]['courseName']} / {instructor_info_dict[offering_id]['sectionName']}\n""")
   
	write_infos(instructor_info_dict, 'instructor.json')

	detailed_info_dict = get_relevant_data(offerings)
	write_infos(detailed_info_dict, 'media.json')
	# pull the actual transcriptions and download to file
	# for offering in offerings:
	# 	pull_offering_transcriptions(offerings[offering])

main()
