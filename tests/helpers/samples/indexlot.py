from tests.helpers.dumps import read_emu_extract
from tests.helpers.samples.image import SAMPLE_IMAGE_ID

# this is taken from ecatalogue.export.20231008.gz but with the MulMultiMediaRefs
# replaced with a single reference to the SAMPLE_IMAGE_ID
raw_data = f"""
rownum=2712
irn:1=1176577
SummaryData:1=[Index Lot]: 19a, L014918709; Vindex sculptilis Bates, 1886 -- Passalidae; Scarabaeoidea; Coleoptera; Insecta
ExtendedData:1=1176577
ExtendedData:2=
ExtendedData:3=[Index Lot]: 19a, L014918709; Vindex sculptilis Bates, 1886 -- Passalidae; Scarabaeoidea; Coleoptera; Insecta
ColDepartment:1=Entomology
ColRecordType:1=Index Lot
GeneralCatalogueNumber:1=irn: 1176577
EntIndIndexLotNameRef:1=922005
EntIndIndexLotNameRefLocal:1=922005
EntIndIndexLotTaxonNameLocalRef:1=989532
EntIndIndexLotTaxonRefLocal:1=Vindex sculptilis Bates, 1886 -- Passalidae; Scarabaeoidea; Coleoptera; Insecta
EntIndIndexLotCurrentNameLocal:1=Vindex sculptilis Bates, 1886 -- Passalidae; Scarabaeoidea; Coleoptera; Insecta
EntIndIndexLotSummaryData:1=Vindex sculptilis Bates, 1886 -- Passalidae; Scarabaeoidea; Coleoptera; Insecta
EntIndMaterial:1=Yes
EntIndType:1=Yes
EntIndBritish:1=No
EntIndKindOfMaterial:1=Dry
EntIndCount:1=1
EntIndTypes:1=Type
LocIndependentlyMoveable:1=Yes
LocPermanentLocationRef:1=102167
LocPermanentLocationRefLocal:1=102167
LocPermanentSummaryData:1=1; G2; 19a; Coleoptera; Main; Dry; Origins; South Kensington; Waterhouse West; WW.1.05
LocPermLocationBarcodeLocal:1=L014918709
LocPermLocationLevel7Local:1=19a
MulMultiMediaRef:1={SAMPLE_IMAGE_ID}
MulHasMultiMedia:1=Y
MulPrimaryMultiMediaRef:1={SAMPLE_IMAGE_ID}
AdmPublishWebNoPasswordFlag:1=Y
AdmPublishWebNoPassword:1=Yes
AdmPublishWebPasswordFlag:1=Y
AdmPublishWebPassword:1=Yes
AdmGUIDPreferredType:1=UUID4
AdmGUIDPreferredValue:1=89cfd775-0823-43a2-91bb-44a5d4fe8eaa
AdmGUIDIsPreferred:1=Yes
AdmGUIDType:1=UUID4
AdmGUIDValue:1=89cfd775-0823-43a2-91bb-44a5d4fe8eaa
AdmInsertedBy:1=Malcolm Kerley, Entomology
AdmDateInserted=1999-07-13
AdmTimeInserted=00:00:00.000
AdmModifiedBy:1=Hillery Warner
AdmDateModified=2023-10-05
AdmTimeModified=16:41:54.000
AdmDateRecordModified=2023-10-05
AdmTimeRecordModified=16:41:54.000
SecRecordStatus:1=Active
SecCanDisplay:1=Group Default
SecCanEdit:1=Group Default
SecCanDelete:1=Group Default
SecDepartment:1=Entomology
SecLookupRoot:1=Entomology
"""

SAMPLE_INDEXLOT_ID, SAMPLE_INDEXLOT_DATA = read_emu_extract(raw_data)
