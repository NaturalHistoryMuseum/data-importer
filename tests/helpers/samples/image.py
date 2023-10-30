from tests.helpers.samples.utils import read_emu_extract

# this is taken from emultimedia.export.20230510.gz
raw_data = f"""
rownum=294874
irn:1=214091
DocDocumentRef:1=214091
SummaryData:1=BM000019319 (image/tiff)
ExtendedData:1=214091
ExtendedData:2=
ExtendedData:3=BM000019319 (image/tiff)
MulDocumentType:1=M
Multimedia:1=214/091/BM000019319.tif
MulHasMultimedia:1=Y
MulTitle:1=BM000019319
MulMimeType:1=image
MulMimeFormat:1=tiff
MulIdentifier:1=BM000019319.tif
DetResourceType:1=Specimen
DetMediaRightsRef:1=49
DetMediaRightsRefLocal:1=49
RightsSummaryDataLocal:1=©; The Trustees of the Natural History Museum, London
RepositoryEmpty:1=Y
ChaFileSize:1=203126196
ChaMd5Sum:1=4dc1be2da1473163e021f5e398ff88bc
ChaImageResolution:1=600
ChaImageWidth:1=6638
ChaImageHeight:1=10199
ChaImageColorDepth:1=8
DocIdentifier:1=BM000019319.tif
DocIdentifier:2=BM000019319.thumb.jpg
DocIdentifier:3=BM000019319.120x10199.jpeg
DocIdentifier:4=BM000019319.200x10199.jpeg
DocIdentifier:5=BM000019319.325x10199.jpeg
DocIdentifier:6=BM000019319.470x10199.jpeg
DocIdentifier:7=BM000019319.705x10199.jpeg
DocIdentifier:8=BM000019319.1500x10199.jpeg
DocWidth:1=6638
DocWidth:2=59
DocWidth:3=120
DocWidth:4=200
DocWidth:5=325
DocWidth:6=470
DocWidth:7=705
DocWidth:8=1500
DocHeight:1=10199
DocHeight:2=90
DocHeight:3=184
DocHeight:4=307
DocHeight:5=499
DocHeight:6=722
DocHeight:7=1083
DocHeight:8=2305
DocCompression:1=None
DocCompression:2=JPEG
DocCompression:3=JPEG
DocCompression:4=JPEG
DocCompression:5=JPEG
DocCompression:6=JPEG
DocCompression:7=JPEG
DocCompression:8=JPEG
DocBitsPerPixel:1=8
DocBitsPerPixel:2=8
DocBitsPerPixel:3=8
DocBitsPerPixel:4=8
DocBitsPerPixel:5=8
DocBitsPerPixel:6=8
DocBitsPerPixel:7=8
DocBitsPerPixel:8=8
DocNumberColours:1=552146
DocNumberColours:2=2307
DocNumberColours:3=4770
DocNumberColours:4=8474
DocNumberColours:5=13820
DocNumberColours:6=18126
DocNumberColours:7=22900
DocNumberColours:8=37033
DocFileSize:1=203126196
DocFileSize:2=26036
DocFileSize:3=31425
DocFileSize:4=41579
DocFileSize:5=63126
DocFileSize:6=97230
DocFileSize:7=176373
DocFileSize:8=686263
DocImageType:1=TIFF
DocImageType:2=JPEG
DocImageType:3=JPEG
DocImageType:4=JPEG
DocImageType:5=JPEG
DocImageType:6=JPEG
DocImageType:7=JPEG
DocImageType:8=JPEG
DocColourSpace:1=sRGB
DocColourSpace:2=sRGB
DocColourSpace:3=sRGB
DocColourSpace:4=sRGB
DocColourSpace:5=sRGB
DocColourSpace:6=sRGB
DocColourSpace:7=sRGB
DocColourSpace:8=sRGB
DocMimeType:1=image
DocMimeType:2=image
DocMimeType:3=image
DocMimeType:4=image
DocMimeType:5=image
DocMimeType:6=image
DocMimeType:7=image
DocMimeType:8=image
DocMimeFormat:1=tiff
DocMimeFormat:2=jpeg
DocMimeFormat:3=jpeg
DocMimeFormat:4=jpeg
DocMimeFormat:5=jpeg
DocMimeFormat:6=jpeg
DocMimeFormat:7=jpeg
DocMimeFormat:8=jpeg
DocMd5Sum:1=4dc1be2da1473163e021f5e398ff88bc
DocMd5Sum:2=9f87747e07c551fdd8955147467472c8
DocMd5Sum:3=72d8eddc1e6a16e43d6b9375a0556465
DocMd5Sum:4=dfbcc90efecddaa29c7f8d2864c0e9c5
DocMd5Sum:5=99e258432e7754fc936b1a447a918782
DocMd5Sum:6=c9ff03a4dc7df2510f8519d4334e620b
DocMd5Sum:7=1cf0c21e449ba8dc8ff2832fd6b5c470
DocMd5Sum:8=b2b001211c71d164f20113b014ab8f2a
ExiTag:1=258
ExiTag:2=40961
ExiTag:3=259
ExiTag:4=40963
ExiTag:5=40962
ExiTag:6=257
ExiTag:7=256
ExiTag:8=306
ExiTag:9=274
ExiTag:10=262
ExiTag:11=284
ExiTag:12=296
ExiTag:13=278
ExiTag:14=277
ExiTag:15=305
ExiTag:16=279
ExiTag:17=273
ExiTag:18=254
ExiTag:19=282
ExiTag:20=283
ExiName:1=BitsPerSample
ExiName:2=ColorSpace
ExiName:3=Compression
ExiName:4=ExifImageHeight
ExiName:5=ExifImageWidth
ExiName:6=ImageHeight
ExiName:7=ImageWidth
ExiName:8=ModifyDate
ExiName:9=Orientation
ExiName:10=PhotometricInterpretation
ExiName:11=PlanarConfiguration
ExiName:12=ResolutionUnit
ExiName:13=RowsPerStrip
ExiName:14=SamplesPerPixel
ExiName:15=Software
ExiName:16=StripByteCounts
ExiName:17=StripOffsets
ExiName:18=SubfileType
ExiName:19=XResolution
ExiName:20=YResolution
ExiValue:1=8 8 8
ExiValue:2=Uncalibrated
ExiValue:3=Uncompressed
ExiValue:4=10199
ExiValue:5=6638
ExiValue:6=10199
ExiValue:7=6638
ExiValue:8=2009:01:16 14:41:58
ExiValue:9=Horizontal (normal)
ExiValue:10=RGB
ExiValue:11=Chunky
ExiValue:12=inches
ExiValue:13=10199
ExiValue:14=3
ExiValue:15=Adobe Photoshop CS2 Windows
ExiValue:16=203102886
ExiValue:17=23266
ExiValue:18=Full-resolution Image
ExiValue:19=600
ExiValue:20=600
IptTag:1=0
IptName:1=ApplicationRecordVersion
IptValue:1=2
XmpMetadata:1=<?xpacket begin="ï»¿" id="W5M0MpCehiHzreSzNTczkc9d"?> <x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="3.1.1-112">    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">       <rdf:Description rdf:about=""             xmlns:dc="http://purl.org/dc/elements/1.1/">          <dc:format>image/tiff</dc:format>       </rdf:Description>       <rdf:Description rdf:about=""             xmlns:xap="http://ns.adobe.com/xap/1.0/">          <xap:CreatorTool>Adobe Photoshop CS2 Windows</xap:CreatorTool>          <xap:CreateDate>2009-01-16T14:41:58Z</xap:CreateDate>          <xap:ModifyDate>2009-01-16T14:41:58Z</xap:ModifyDate>          <xap:MetadataDate>2009-01-16T14:41:58Z</xap:MetadataDate>       </rdf:Description>       <rdf:Description rdf:about=""             xmlns:xapMM="http://ns.adobe.com/xap/1.0/mm/"             xmlns:stRef="http://ns.adobe.com/xap/1.0/sType/ResourceRef#">          <xapMM:DocumentID>uuid:3EA82FCEDBE3DD1180EE8DA8F82127DB</xapMM:DocumentID>          <xapMM:InstanceID>uuid:3FA82FCEDBE3DD1180EE8DA8F82127DB</xapMM:InstanceID>          <xapMM:DerivedFrom rdf:parseType="Resource">             <stRef:instanceID>uuid:3CA82FCEDBE3DD1180EE8DA8F82127DB</stRef:instanceID>             <stRef:documentID>uuid:763C96A9DBE3DD1180EE8DA8F82127DB</stRef:documentID>          </xapMM:DerivedFrom>       </rdf:Description>       <rdf:Description rdf:about=""             xmlns:tiff="http://ns.adobe.com/tiff/1.0/">          <tiff:Orientation>1</tiff:Orientation>          <tiff:XResolution>6000000/10000</tiff:XResolution>          <tiff:YResolution>6000000/10000</tiff:YResolution>          <tiff:ResolutionUnit>2</tiff:ResolutionUnit>          <tiff:NativeDigest>256,257,258,259,262,274,277,284,530,531,282,283,296,301,318,319,529,532,306,270,271,272,305,315,33432;42BF0997832335EAE016DC544907B475</tiff:NativeDigest>       </rdf:Description>       <rdf:Description rdf:about=""             xmlns:exif="http://ns.adobe.com/exif/1.0/">          <exif:PixelXDimension>6638</exif:PixelXDimension>          <exif:PixelYDimension>10199</exif:PixelYDimension>          <exif:ColorSpace>-1</exif:ColorSpace>          <exif:NativeDigest>36864,40960,40961,37121,37122,40962,40963,37510,40964,36867,36868,33434,33437,34850,34852,34855,34856,37377,37378,37379,37380,37381,37382,37383,37384,37385,37386,37396,41483,41484,41486,41487,41488,41492,41493,41495,41728,41729,41730,41985,41986,41987,41988,41989,41990,41991,41992,41993,41994,41995,41996,42016,0,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20,22,23,24,25,26,27,28,30;10BCF593F34C21EA681C50AE1C995567</exif:NativeDigest>       </rdf:Description>       <rdf:Description rdf:about=""             xmlns:photoshop="http://ns.adobe.com/photoshop/1.0/">          <photoshop:ColorMode>3</photoshop:ColorMode>          <photoshop:ICCProfile>Adobe RGB (1998)</photoshop:ICCProfile>          <photoshop:History/>       </rdf:Description>    </rdf:RDF> </x:xmpmeta> <?xpacket end="w"?>
GenDigitalMediaId:1=0d5f124013467e40307c6e0dc7595cd92d25b907
DetBornDigitalFlag:1=n
NhmSecOpenDataPolicyException:1=none
AdmWebMetadata:1=BM000019319
AdmPublishWebNoPasswordFlag:1=Y
AdmPublishWebNoPassword:1=Yes
AdmPublishWebPasswordFlag:1=Y
AdmPublishWebPassword:1=Yes
SecCanDisplay:1=Group Default
SecCanEdit:1=Group Default
SecCanDelete:1=Group Default
SecDepartment:1=Botany
AdmGUIDPreferredType:1=UUID4
AdmGUIDPreferredValue:1=c2bde4e9-ca2b-466c-ab41-509468b841a4
AdmGUIDIsPreferred:1=Yes
AdmGUIDType:1=UUID4
AdmGUIDValue:1=c2bde4e9-ca2b-466c-ab41-509468b841a4
AdmInsertedBy:1=KE User acct.
AdmDateInserted=2013-11-12
AdmTimeInserted=16:13:51.000
AdmModifiedBy:1=Darrell Siebert, Zoology
AdmDateModified=2016-02-03
AdmTimeModified=09:13:10.000
"""

SAMPLE_IMAGE_ID, SAMPLE_IMAGE_DATA = read_emu_extract(raw_data)
