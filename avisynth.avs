LoadCPlugin("C:\Program Files\AviSynth 2.5\plugins\ffms2.dll")

global introOutro = {introOutro}
global brandClips = {brandClips}
global framerate  = {fps}

introOutro ? Eval("""
	intro = FFmpegSource2("{intro}", atrack=-1, fpsnum=framerate).ConvertToYUY2()
	outro = FFmpegSource2("{outro}", atrack=-1, fpsnum=framerate).ConvertToYUY2()

	#intro = SubtitleEx(intro,UCase("{title}"),font="Neo Sans Std Medium",textcolor=$1B4184,halocolor=$FFFFFF, size=58,x=340,y=470)
	#intro = SubtitleEx(intro,UCase("{course}"),font="Neo Sans Std", effects="",textcolor=$1B4184,halocolor=$FFFFFF, size=34,x=340,y=420)
	#intro = SubtitleEx(intro,"{date}",font="Neo Sans Std",effects="",textcolor=$CCCCCC,halocolor=$FFFFFF, size=36,-20,y=-20)

	introtext = ImageSource("{introoverlay}")
	intromask = ImageSource("{introoverlay}",pixel_type="RGB32").ShowAlpha(pixel_type="YUY2")
	intro = Overlay(intro, introtext, mask=intromask)

	outro = overlayImage(outro, "{outrooverlay1}", 0, 0, 10, 0)
	outro = overlayImage(outro, "{outrooverlay2}", 65 , 120, 10, 10)
	intro = FadeOut(intro, 10)
""") : ""

function overlayImage(clip base, string image, int startframe, int endframe, int fadeStart, int fadeEnd) {{
		endframe == 0 ? Eval("""
			actualEndFrame = Framecount(base)
		""") : Eval("""
			actualEndFrame = endframe
		""")

        imageClip = ImageSource(image, end=(actualEndFrame-startframe), fps=framerate)
        imageMask = ImageSource(image,pixel_type="RGB32", end=(actualEndFrame-startframe)).ShowAlpha(pixel_type="RGB32")

        fadeStart > 0 ? Eval("""
        	imageMask = imageMask.fadeIn(fadeStart)
        """) : ""
        fadeEnd > 0 ? Eval("""
        	imageMask = imageMask.fadeOut(fadeEnd)
        """) : ""

        begin = base.Trim(0, startframe-1)
        middle = base.Trim(startframe, actualEndFrame)
        end = base.Trim(actualEndFrame+1, 0)

        middle = Overlay(middle, imageClip, mask=imageMask)

       	(endframe == 0 && startframe == 0) ? Eval("""
        	output = middle
        """) : ""
        endframe == 0 && startframe != 0 ? Eval("""
        	output = begin ++ middle
        """) : ""
        startframe == 0 && endframe != 0 ? Eval("""
        	output = middle ++ end
        """) : ""
        startframe != 0 && endframe != 0 ? Eval("""
        	output = begin ++ middle ++ end
        """) : ""

        return output
}}

function brandClip(clip c) {{
	logo = ImageSource("z:\home\typothree\VideoParts\logo.png")
	logomask = ImageSource("z:\home\typothree\VideoParts\logo.png",pixel_type="RGB32").ShowAlpha(pixel_type="RGB32")
	Overlay(c, logo, 1160, 40, mask=logomask)
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
	intro ++ {videoList} ++ outro
""") : Eval("""
	{videoList}
""")
