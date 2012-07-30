LoadCPlugin("C:\Program Files\AviSynth 2.5\plugins\ffms2.dll")
LoadPlugin("C:\Program Files\AviSynth 2.5\plugins\TransAll.dll")

global introOutro = {introOutro}
global brandClips = {brandClips}

introOutro ? Eval("""
	introraw = FFmpegSource2("{intro}", atrack=-1, fpsnum=25)
	whitebox = BlankClip(120,width=380,height=85,color=$FFFFFF)
	intro = Overlay(introraw,whitebox,x=335,y=380,mode="Blend",opacity=1)
	intro = Overlay(intro,whitebox,x=900,y=635,mode="Blend",opacity=1)
	outro = FFmpegSource2("{outro}", atrack=-1, fpsnum=25)

	intro = SubtitleEx(intro,"{title}",font="Ubuntu",textcolor=$323586,halocolor=$FFFFFF, size=50,x=340,y=450)
	intro = SubtitleEx(intro,"{course}",font="Ubuntu",textcolor=$323586,halocolor=$FFFFFF, size=25,x=340,y=410)
	intro = SubtitleEx(intro,"{date}",font="Ubuntu",textcolor=$CCCCCC,halocolor=$FFFFFF, size=30,-20,y=-20)
	intro = FadeOut(intro, 10)
	subtitleex(intro,"test")
""") : ""

function brandClip(clip c) {{
	logo = ImageSource("z:\home\typothree\logo.png")
	logo = GreyScale(logo)
	logomask = ImageSource("z:\home\typothree\logo.png",pixel_type="RGB32").ShowAlpha(pixel_type="RGB32")
	Overlay(c, logo, mask=logomask, x=1183, y=20, opacity=0.8)
}}

function addVideoClip(string s, int start, int end) {{
	clip = FFmpegSource2(s,atrack=-1, fpsnum=25).Trim(start, end)
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
