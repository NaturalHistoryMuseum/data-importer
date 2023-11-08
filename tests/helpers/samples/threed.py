from tests.helpers.samples.dumps import read_emu_extract

# this is taken from emultimedia.export.20211117.gz
raw_data = f"""
rownum=59
irn:1=3709063
DocDocumentRef:1=3709063
SummaryData:1=3D Visual of PV M 82206 C (a) on Sketchfab (x-url/application)
ExtendedData:1=3709063
ExtendedData:2=
ExtendedData:3=3D Visual of PV M 82206 C (a) on Sketchfab (x-url/application)
MulDocumentType:1=U
MulHasMultimedia:1=N
MulTitle:1=3D Visual of PV M 82206 C (a) on Sketchfab
MulCreator:1=NHM  Image Resources
MulMimeType:1=x-url
MulMimeFormat:1=application
MulIdentifier:1=https://skfb.ly/oqKBO
DetPublisher:1=Sketchfab
DetResourceType:1=Specimen
DetMediaRightsRef:1=49
DetMediaRightsRefLocal:1=49
RightsSummaryDataLocal:1=©; The Trustees of the Natural History Museum, London
RigRightsTypeLocal:1=©
ChaRepository:1=KE EMu Repository
RepositoryEmpty:1=N
RelIsParent:1=No
DetBornDigitalFlag:1=n
NhmSecOpenDataPolicyException:1=none
AdmWebMetadata:1=3D Visual of PV M 82206 C (a) on Sketchfab NHM  Image Resources
AdmPublishWebNoPasswordFlag:1=Y
AdmPublishWebNoPassword:1=Yes
AdmPublishWebPasswordFlag:1=Y
AdmPublishWebPassword:1=Yes
SecRecordStatus:1=Active
SecCanDisplay:1=Group Default
SecCanEdit:1=Group Default
SecCanDelete:1=Group Default
SecDepartment:1=Palaeontology
AdmGUIDPreferredType:1=UUID4
AdmGUIDPreferredValue:1=5ab7511b-8999-4181-b1ba-c843bc50b3d5
AdmGUIDIsPreferred:1=Yes
AdmGUIDType:1=UUID4
AdmGUIDValue:1=5ab7511b-8999-4181-b1ba-c843bc50b3d5
AdmInsertedBy:1=Chris Dean
AdmDateInserted=2021-11-16
AdmTimeInserted=16:00:40.000
AdmModifiedBy:1=Chris Dean
AdmDateModified=2021-11-16
AdmTimeModified=16:02:53.000
"""

SAMPLE_3D_ID, SAMPLE_3D_DATA = read_emu_extract(raw_data)
