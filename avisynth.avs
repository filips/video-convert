LoadCPlugin("C:\Program Files\AviSynth 2.5\plugins\ffms2.dll")

global introOutro = {introOutro}
global brandClips = {brandClips}
global framerate  = {fps}

introOutro ? Eval("""
	intro = FFmpegSource2("{intro}", atrack=-1, fpsnum=framerate).ConvertToYUY2()
	outro = FFmpegSource2("{outro}", atrack=-1, fpsnum=framerate).ConvertToYUY2()

	intro = SubtitleEx(intro,UCase("{title}"),font="Neo Sans Std Medium",textcolor=$1B4184,halocolor=$FFFFFF, size=58,x=340,y=470)
	intro = SubtitleEx(intro,UCase("{course}"),font="Neo Sans Std", effects="",textcolor=$1B4184,halocolor=$FFFFFF, size=34,x=340,y=420)
	intro = SubtitleEx(intro,"{date}",font="Neo Sans Std",effects="",textcolor=$CCCCCC,halocolor=$FFFFFF, size=36,-20,y=-20)
	intro = FadeOut(intro, 10)
	subtitleex(intro,"test")
""") : ""

function brandClip(clip c) {{
	logo = ImageSource("z:\home\typothree\logo.png")
	logo = GreyScale(logo)
	logomask = ImageSource("z:\home\typothree\logo.png",pixel_type="RGB32").ShowAlpha(pixel_type="RGB32")
	Overlay(c, logo, mask=logomask, x=1183, y=20, opacity=0.8)
}}

function addVideoClip(string s, float start, float end) {{
	clip = FFmpegSource2(s,atrack=-1, fpsnum=framerate, threads=1).ConvertToYUY2()
	clip = Trim(clip, Round(start * framerate), Framecount(clip) - Round(end * framerate))
	brandClips ? Eval("""
		clip = brandClip(clip)
	""") : ""	
	ResampleAudio(clip, 48000)
}}



introOutro ? Eval("""
	AlignedSplice(intro, {videoList}, outro)
""") : Eval("""
	AlignedSplice({videoList})
""")
