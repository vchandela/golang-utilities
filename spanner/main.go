package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

type dur struct {
	prod int64
	temp int64
}

func main() {
	// Parse input.txt into map for easy lookup
	inputFile, err := os.Open("input.txt")
	if err != nil {
		log.Fatalf("Failed to open input.txt: %v", err)
	}
	defer inputFile.Close()

	duration2Map := make(map[string]dur)
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) != 3 {
			log.Printf("Skipping invalid line: %s", line)
			continue
		}
		livestreamID := strings.TrimSpace(parts[0])
		var dur_prod int64
		_, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &dur_prod)
		if err != nil {
			log.Printf("Skipping line with invalid dur_prod: %s", line)
			continue
		}
		var dur_temp int64
		_, err = fmt.Sscanf(strings.TrimSpace(parts[2]), "%d", &dur_temp)
		if err != nil {
			log.Printf("Skipping line with invalid dur_temp: %s", line)
			continue
		}
		duration2Map[livestreamID] = dur{prod: dur_prod, temp: dur_temp}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading output.txt: %v", err)
	}

	// Create a Spanner client
	ctx := context.Background()
	projectID := "moj-prod"
	instanceID := "livestream-instance"
	databaseID := "production-db"
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()

	query := `
		SELECT livestream_id, (ended_at - created_at) / 1000 AS duration_in_sec
		FROM livestream
		WHERE livestream_id IN UNNEST(@livestreamIds)
		AND created_at > @createdAtTimestamp
	`
	livestreamIDs := []string{
		"00038551-a1b8-4fa4-be54-506a7c0bf4c5", "00f7ba28-4fb2-446a-b564-39cbf0658e75", "0186671b-558a-4dca-bae0-c831ae21db40", "014d7b8c-deb8-4500-8caf-960fc9d9123b", "01e6ab53-263d-4ed4-87ab-117541aeb760", "01f30b14-6d23-4204-8494-8375fbc5ca27", "01f5746f-0a1f-46c4-a97c-a27fe82cf4b4", "027a6e3c-d45d-44a7-bffc-78ab2effec21", "04960c6f-f5e9-472d-ba5a-448ad60c5ac9", "051f0733-7b0a-4b52-bbce-8c6b333cc63e", "05e3384f-1037-4e7d-9dcf-1707dfd908d9", "06cf896d-0ab2-47b4-8b1e-2180717140df", "0690e627-4419-4a3c-a05f-f0e4ef624910", "06f7a438-30e8-4685-aaee-fd739f690781", "06ce9e03-0794-4388-85a4-f378b0af1302", "0726d463-49c8-42d1-ab95-9abdd2647c8b", "085c7cb2-9546-4888-961a-7a8ce7f46199", "0886d120-9517-4d09-a19e-6ad6caa50196", "09dde535-542c-4502-b05a-d4ddf8c3252a", "0ae787ad-5408-4ca5-a14c-24570185c92b", "0b25f5a4-8354-4a57-b7b1-46483d30b6d8", "0ba77749-e23b-4cc2-aaa7-7cabd9bb03d2", "0b6d7798-c37d-4edd-b50f-2090aeafa47e", "0cee74eb-d0eb-47fb-a066-5de83d1b2ef7", "0e18a3e0-0c1f-41a8-aa0f-5844710cd0de", "0dd57cd9-e550-418b-a247-6301d4778390", "0ed89f5a-ef48-464b-a051-bee2ce0eba46", "10a7639d-d2b8-48f7-a862-57f73f617b7f", "1038fe47-dc0e-460e-bb02-cc0624c59b95", "10763044-2707-4f53-9408-55aca7c313d4", "1149957d-75aa-4397-87af-3c49d7281bbe", "12cc11da-913e-4936-a7d3-aba9566cf1f1", "134b3b84-3c95-4161-9b99-45f324e2dc3a", "142ec6b8-d8e4-4655-8a2b-6dc811474195", "16dfea13-e7fd-4597-ba74-578d60c7e91c", "17298f60-b9f8-4b61-a194-2e91efbef52b", "1741662b-0be2-4d63-ab19-6636e8fc39da", "174cc1a0-9540-4ca8-87e1-074d5507de21", "17a2e31b-c228-4c8d-88d4-b376532e3ca5", "17eb6b5b-31cc-425a-b266-03c7590b30f8", "18c7452c-148c-4544-8458-8eae550e517f", "198b2ca8-9900-4d96-9594-0564eaf1249f", "1b10c579-7229-4a87-9059-515a12b91a29", "1a684190-8a93-4eba-9849-62342dbe0009", "1ca352f6-70d2-45f0-8ef1-ccc2c6cf6a8c", "1cf5ab7f-ab27-4552-b6e7-0637d9f7f6d7", "1dbd0bcc-c01f-4875-974d-785b41b36b72", "1e1a7bd0-3efb-4e4f-94aa-4aeca4f01a23", "1eb5b1ff-0ecc-46d3-a4e7-3d8fe12d1ed8", "1fb8d003-f7f8-44f5-af4f-b118bb42cc16", "20cbc7e4-c944-42f0-b4e8-6b0aa9529bc0", "214723d3-32ce-4fb2-840d-9157623c94bd", "1fb7afe6-f9c7-4bb5-9bbd-c2ba5834716d", "20f50a6e-0b6c-40d3-8a49-21bf3695fa0b", "20a7debb-6d84-4cb0-a465-aabe07fcab5a", "21fbbac4-610c-429b-b478-fe2aea93d68a", "2233afb0-fe7a-48a7-9ef9-116cc4eed01f", "22bc4728-cac1-4ca4-bdd2-4c63dd32fa1a", "24706607-2b7c-4686-8e71-4c48e75797d7", "24658ad8-4cb0-401e-a62a-7045c4a841e0", "24e0f56b-495b-430b-a10e-fc4ae71667e1", "24ae49fa-e4c6-49e1-9d08-490a93e14142", "256d7d08-ceea-462b-adea-a45cf5c94838", "25aca9e2-4e66-4c78-ab83-295580187bd3", "25e50976-6ae1-4905-a0b1-76cde53cdb01", "270b3cf2-321a-4953-a3c6-e3c03d9aacaa", "272e2c48-81f1-4bac-8df5-2ce82e972a2a", "27183b36-c085-4b8d-849e-95bcade1cd7d", "27ae657d-3cfd-4d50-bfc6-0f103037207b", "27cb4315-5f67-4966-b831-1ae3b3d7d632", "282aaf47-84a6-4a9f-ac33-2c3b3e42dcc5", "2945fea4-439e-4c40-9c36-24d85885f2e3", "292cfe5b-5fac-47ab-97d1-9e67e4b3277f", "28f504e2-7a76-424b-9321-23a3c4c17857", "2b715021-7f83-480f-a7f6-5e3e061a6198", "2ba4d03a-c696-44b2-b8d4-d8ef010f4fa3", "2a694660-4c22-4238-bf0b-ea645967fc39", "2de4915a-0055-4a3a-84ef-72bdb17db3d0", "2eeb9884-2bfe-4d91-8385-9dc20fb0146e", "2e564e73-448d-406b-8b19-cbc9ef578c59", "2f1b5947-6d3f-463a-9ebf-ec134eb686f3", "2eefaecb-291a-407b-9d55-3b6e1b200c5d", "2e0ab62e-1d1e-408e-b834-051648006a7e", "3109f120-e2e8-4c61-8b36-8f957cfb0777", "31fb0162-8b46-46c5-920c-1312885dd3d2", "3243a43a-9ed8-44d4-816c-a725e1beab4d", "3205b4fb-fee4-42a3-ab62-8d0df8d2715c", "324ce679-bf00-4b45-8774-549c5d382fd7", "31b4d5b5-e0f9-4caf-864d-68c84c00f4c2", "33dc8db2-80ab-4e6c-a406-e7ed8bfedea3", "3544aa1e-e85f-464b-a223-ba8f20bc56f0", "3585e59e-ab58-49e1-8e07-2df39a3addbe", "359d2155-149d-4819-8b44-c3667e08a60d", "3687aa4c-799c-41b8-8b51-0e0e0bca0417", "371e15c8-8be3-480d-b181-477f8f1fec2c", "386fb731-dc2e-4d88-8997-23b4fc32508e", "39185073-b40f-47db-a118-8fb0924ef774", "388be4de-4876-46ed-9d96-f2ce727d12d0", "38f49f91-d115-4ec1-832a-e9f70b77417f", "39c669be-3ed2-407c-bfe8-23f0d1d6b266", "3a18fb5b-913b-4e3b-892a-4241af89e563", "3a728600-b4a5-446e-bbbf-b625b13b6c23", "3a5207d5-f131-4340-b83f-c661f35686e7", "3a7fa49f-fae3-4b51-9d7f-4ec40199f878", "3b800012-b2b3-4e04-9530-8275decd4f7a", "3c4eaa65-18d3-4438-8a1c-d657b5cabdeb", "3cfb7b2c-2a3d-4bf6-90ee-154d70558f8d", "3c2c7cf7-44d9-48eb-b208-f467f931853e", "3d10a005-9d5d-4da8-a535-0805c46b289d", "3d991ab5-1c41-4d8a-b3a7-8a6cd769701a", "3f4f74d6-10ff-4668-a04c-199c7b4addb0", "3f52d6a0-f2f4-4e78-9cd5-9109e235cc4a", "3fd16cc5-6948-4916-b2fb-1beb24dc6056", "4086cdd5-f379-44bd-b80d-b2ccb8aec223", "417f5d3f-92af-476a-89ec-137ce9fe52a3", "41508a01-e1c1-44c9-8d74-8f9ae5baafb1", "41ab94b3-fc2d-4b25-85a1-eba855920a9f", "41cf7cfa-9fa2-406a-8328-e3eb4c798ef3", "41ba8617-497a-41b9-bdef-ac70097d15de", "433237c2-864c-43b8-a332-901376ce57b3", "43b2389d-8c64-40fd-b5cd-ffbecbab810d", "4361d864-bfc2-4188-8f78-127e5d925847", "432f4e50-5700-408a-b1fc-0cbc3424b2a8", "441d6c51-c08a-4a18-b101-d0420f115243", "44f7bb4f-da01-4955-9d20-e6574598c2d1", "46f8a067-149d-4c20-b2b7-dc898ffaf0a4", "476281ce-04de-415f-bb98-b07ea2d5bd9b", "470ae35c-009f-4ce3-a943-79045c613fa0", "475501cb-5e44-4ad9-9eab-5befa722101e", "478c88ea-d204-4230-894c-881fd8288895", "47869626-ad16-47a9-9cd1-5cdbe8b50086", "4801341d-abfb-4d7d-b173-bec1fd10a5a4", "48473789-42ff-46e6-acd6-fe3a0a9b6ffa", "48fbc387-4760-472e-82d1-6dff9a817d17", "49bbd42e-851c-43eb-a755-20f69638a97c", "4a36e719-06b9-4e0b-9673-b1b70bf22fc7", "4a754c40-569e-4560-a745-a415c613970e", "4aa49441-5c84-461a-8d2e-65c95f0e6f2e", "4b24f9aa-4c26-4176-8c27-a18136a3fc72", "4b69ce4d-cbe0-42ad-9f49-cb63f33c6e08", "4bd4af67-825d-41e0-ad9b-dc08281fd92c", "4c5d89d3-4710-47b4-aae2-3f415df9223c", "4c3070a1-9a7c-443b-a585-c3e0ff2a0e1c", "4c7b5c79-56be-47b4-b779-ae9d7d5c7124", "4cf91c2e-c110-4f93-9a3d-cb86bae1c822", "4ceaf161-2ac0-4039-987a-fdcc7566788d", "4d83bbf4-11da-4abe-ae13-eb0cd35286ad", "4d7fac1a-5edd-4742-bdfb-2e3485b986e7", "4d8d887b-e42b-420a-8982-3bdef23409fd", "4d7de198-6f1c-4335-b61c-10d85a7daf71", "4e56331c-27b8-4fa0-88ca-bfd068f25aef", "4f2e3bfa-f840-4b62-8eb0-9f70c2741daf", "4f7f272d-ff4e-4de0-b6f7-7e6c26d00232", "4f680e7b-032b-49ef-857b-62346883fd7f", "50174acf-93ba-40db-bbf7-297fb1cd986c", "5018bb65-d8d1-451c-8a98-e2197f62e81c", "5016ae51-8d75-479e-9003-25b0a8737409", "50dc3d99-1ab2-418f-924e-7fecd9e79a9a", "51843ba3-07ce-43b9-9553-9ef1aea3f6b5", "51375edd-3398-4b35-a298-46ef7c0baaa7", "512458f4-8b7d-4da7-883c-3baaa055112f", "52132ac4-faad-4908-9bc9-e37e037cef45", "52236f6d-1a0b-44ad-8c12-8d703fc5afa2", "529039aa-3802-4b4f-982f-a0ebb5b6a217", "531086a3-cd9e-42bb-bbc1-0986b0ea27ba", "53916386-2a97-4f93-81bc-22ba50d0d2ec", "533aa68e-0d71-4d49-9168-ada639d84c40", "53c6d239-359e-48e5-bb24-d48698b85a76", "56874f90-f9e7-4add-917d-590a089518c4", "5a5b73bf-3fa9-43e4-9b2e-00114fca0533", "5a23f620-ad8f-46df-af9d-2e6b95c756ae", "5a820f14-7182-4d59-9db3-c602a78c2b9c", "5ad97eda-cf85-4eff-8579-e50c3494c814", "5b324845-5bbf-427e-9bac-cefa7805854b", "5ba8c68d-ad0b-4375-87bf-d0fb91de370f", "5c3e4336-2e92-46fc-8ad6-d56ac78e7bf3", "5c8b0d94-5dfa-4131-a0fe-34ba21208c6b", "5cffc5d1-74bf-461e-9619-d9886f1a3acb", "5d096078-7744-43f5-af9c-698d25be2f83", "5e202694-fb5f-4081-a364-de3c453a1fef", "5dcd7a8d-4ac3-404e-913f-b40dad82e8fd", "5e4e1c1e-0f77-43e8-b155-2e1969d7d836", "5df38a53-c753-46d3-a75c-f11488a0c04b", "5f242b1a-8870-4b98-9f46-1633e81896a2", "6017fecf-6a4e-451f-9c94-911299782d44", "60ce1dee-8189-43da-89a7-956d06150678", "61475049-9676-4845-9523-469725d96f85", "61f6381f-4bd6-429f-bebb-7ff4dc3b51fc", "6292c763-9e07-4523-8819-fbec7b6a8a64", "62669f2c-aabc-4c6b-b4a6-80d31f462bbc", "634ec911-704e-4379-b4f0-55409786244f", "62e0e0d5-9b28-45ca-9dfd-5571846c7231", "642ae4b5-5940-4220-9312-fd67f9cc5742", "64c88245-d92d-4694-ab21-83b5a3c8524b", "65c702e1-6029-49b8-8afa-198fc7080170", "6602632d-a740-4d93-89e9-aebc5bf7bf6c", "6555afc8-db81-4311-a0c0-fa73c8271212", "6683d5df-6b8a-4d83-adb7-1b3d751c1b6b", "68158d82-8720-4b07-a4a7-f19814e07af9", "680cedad-7b26-47e3-a940-2d515707d1e8", "6990022c-47e4-4927-910e-b67a067b495a", "69cb171e-099e-4598-b38a-228aec399032", "6a2cf953-6ec1-463e-a366-0e767f5eb656", "693cc5b4-97b0-4083-ab9a-8bf7a793da07", "69eec01b-a0d4-4e50-84c2-0e564d36f2f1", "6a8c5c06-516f-43c3-8996-bcb17925d947", "6b147a63-73e4-4f24-8ec6-a504e5dcaeed", "6bb8ffad-bb43-4f46-8c27-c52802287064", "6b4585e3-0197-4443-aec7-8069f440c53b", "6bfe3d77-5e11-4f9c-972d-34cfff5b4868", "6bd1d6bf-e4a6-4f8c-922d-86beddca6ce3", "6d34c0b4-c009-49a8-bd4d-1923a5fb370a", "6d848717-aaf6-4106-8ec7-bffb0c30d953", "6e1a5e2d-a2d2-4a34-950a-0cafc138fe8e", "6dcfd975-a408-48e1-b84d-963cc21f8849", "6e7d1401-b6b6-426a-ace1-5c65a375f8f7", "6dd510fb-2797-4d79-86a1-6c249b59e726", "6f8b997c-3a8f-448a-b9f9-c69ea7abfa47", "6ecbc64a-3579-4b0f-a90a-81d3f03645e9", "6fe43251-31f3-4110-9ab1-e0664a0d1891", "709e1b5d-e2ca-4778-9c40-2ea43cc97cd1", "7065efe4-aaee-4c8d-aaeb-8c46df3ba3ae", "7117f736-dafc-4635-9e62-eb65aea3aba3", "7139d661-8b56-4add-bd64-7af72aa5b9dd", "7092cb26-2cb2-4f78-a6dc-e8d573e5c55a", "71b9269b-dcde-43be-8fa9-36fc82cc47d4", "71eb8eee-afe5-4346-aa23-3ecc47420827", "726c4c5d-9245-4b98-8550-5b5ac4c3a124", "71cae2bc-7b35-4930-ad49-a4cfd2a2d43e", "726e700b-441a-4869-8bc5-5c35fee26549", "7200703b-69e4-4a18-a4a4-776e4db74b3d", "7488430f-623c-4d89-bb8a-af73d5e9975c", "74501efe-48a9-47a5-86b7-bebafc4ca020", "73ec0ff7-a6b1-4ee2-aaf9-f81662e92ac6", "74e313b4-7a27-4f1c-b248-775352a96e8d", "75adb0c9-d015-4ea5-905a-76be13005edd", "757ba083-1a24-479c-bbdf-2d7b69a903bd", "762f9edc-01fd-4269-84b5-c91674f711c9", "768a8298-e7af-41ba-81d1-8e8c15e9324c", "764b4e5a-5144-41f3-9b2c-2a030a5cca12", "76574533-015c-46e0-83fc-4c298d9fc6fc", "769f6985-1959-473c-9abf-434508c1d286", "776e07b1-d693-4f60-b825-264ed4fa22cf", "77c09515-490b-45dd-b4da-64d8bb8ca9ed", "78a3ce1b-55d7-48b8-a0f1-06337cf5ce51", "78d38d35-05cc-4d1d-af86-3be674057113", "79d3a588-31c0-4695-ba42-a7454d4c8432", "7a4ecf5e-990a-4380-a2e4-8049b7afc95c", "7aa16299-9929-4443-8da3-9ced33cb1b3b", "7b93064c-c12d-4b8f-89b5-05ae024f0a48", "7ca4aaf7-d184-4714-8604-a92085fc89c3", "7c92e648-50c7-4795-bdb9-c3b6f737f357", "7dfc96a5-195a-43a5-a7e6-99684047df69", "7d0d51d0-fb29-40c9-8593-82bf6edc92be", "7e36e298-f968-43ff-b74c-58050a9e14e5", "7ec3d067-10ca-4cd7-9b13-cac7c1d42adc", "806e8f2e-786b-4cde-b14f-37a29cdd950b", "80748ada-f7cb-454c-ba4c-46e8a00a8b82", "80bb0a6b-76d2-4206-bd44-84ae76c67bf0", "80bb766e-2b3a-4d9a-ad57-72324260bd74", "8134c63e-60ec-48eb-938d-32f47696706e", "81d964f6-be37-4ff7-970e-c32662123d00", "829b8c31-a246-41c2-850a-105a7cc22ff3", "82dce34b-75b4-48ed-a36f-6d073c1f1ac8", "840196dd-4921-4ac9-97ed-1b11daa3268d", "848eb2e6-3a68-4baf-b628-3fa47e093a1b", "846624a8-5c4d-405f-9120-84ff2074df93", "83a9bf67-7f34-4cee-8bf8-dbd95ec620d2", "843cadb8-a98b-45d0-bf72-f6536c05388a", "868eefb4-5522-49fb-ac78-91a732a7d9fe", "86bf32b1-46e3-46c7-981a-2129e12ae524", "86f61689-d96e-4405-99b1-ecfd232d93c8", "8745a682-f566-4125-b3b4-5d486187d522", "8774dfc4-d585-4422-b471-3ce612c0dee2", "8846fa34-3b42-4fd4-b4d9-26592007298e", "883a18ed-ae16-40af-ad13-85df1a0054c0", "882ae9fe-2507-4345-a472-03fee933ae18", "889966ea-8860-441c-96c5-ddf35484c502", "89dceb58-04b8-4868-ac01-4d38f17fb57b", "8ad4ff61-bb58-4a65-aac5-c87fb3548bf9", "8c4e0e46-14be-40cf-8e21-ac13c6441df9", "8d540cd8-ba0b-4c14-a73e-d9087b34a922", "8ca6f5f5-a28e-4f28-bcff-89b594b7e1a5", "8daa9974-0789-4601-a2d9-c40ec500059f", "8e7597ac-2170-45cb-b56a-89f2afced70a", "8f3aa603-92c1-4b15-b34f-c5460e1f5147", "8ed1788a-b651-4235-b8b5-5f2f50a39de2", "9086cbf7-7625-41f5-b4c1-c1b595ae3a5e", "916c9981-7b4b-42dd-bdbc-0ffa6d1b10b6", "91e0e70f-8a2c-4b4c-8f44-36148d7e20e2", "926ad12d-e488-4a94-a562-df35f7638054", "92397fde-ce08-434c-9678-a8d8582d2204", "93f4f8b4-c993-431e-bc01-be4065748b9f", "94528704-ee89-4fc6-8333-0703b1b66139", "94dec47c-6651-4b00-8c88-ec51cebbbed0", "94b42d12-e219-4f6b-8751-808a50f0fcd1", "9698806a-1238-4257-b4fc-cadeed1e2975", "95a12f76-dfdd-4121-9f63-63161dcb677d", "96df0f8d-0095-47be-8cdf-085784a86826", "9758036a-bade-4e2c-9018-3bd69dac7cf0", "9773b52c-4f5e-43ec-a7ed-7a09acdf8a24", "98ac418b-c75b-4510-995d-194c827fa1cd", "987205ab-7a0e-42e7-83b0-58cb9d5a420d", "98d60d2b-470d-455d-8d8f-109baff03a02", "98a9b277-b535-45c4-b858-f1ad09080d91", "991a21ed-5cb7-4bef-9fc8-339bdb1dd51c", "98dd36fb-a881-4cdc-8ed4-15cf8d81f350", "9a3705c3-9075-42fb-939a-7d855ece053a", "99a0e1b8-c0fa-4b15-a0d1-2ee70d0245a2", "9b9143e8-9395-4291-a260-2d95400bc36b", "9b78e46b-d886-4bba-876a-8e517795e00e", "9c3a38a9-3b99-4055-b295-8485ee318e70", "9b982ab0-2211-4228-b16a-01c22f9c5042", "9d891c5a-b3cd-4fff-88d2-c0442719ca10", "9d96442e-a041-4284-b4d4-f7a1246df55f", "9dc6972c-7941-497a-97dc-5a036bdf8fd9", "9d90f358-34fc-4ec2-8ef5-d17128fa989c", "9e496e31-e23c-480d-a702-05b9df59da75", "9f397723-2756-43c0-a2ab-d38808594552", "9efa0e06-c7f6-498b-a181-42dbe50f1044", "9f1d9251-5769-4d06-87b4-0fc6270621c6", "9f9ee50d-7332-4335-a683-7c66e2dda2ae", "a02699d4-2459-462d-bb32-c2a462fed1f8", "a064ef05-1ecc-4fd8-af3a-c0a0de3cb2d2", "a05840d3-eda8-406a-b85b-b0a0578d2285", "a0b6319e-4e33-4f20-89a4-ad64bb2c82b6", "a1bbe0c9-7e26-4b32-a0f7-79dd2a6363f5", "a231cb99-d98c-4fb0-bd47-fe48166d2b19", "a28b3526-5e8a-41de-8e26-e2dc7eaa2850", "a32564f4-4e34-4878-9acb-288e1be46b01", "a4298b62-c75a-4116-968d-a8184f670b12", "a59ccb4f-e2c7-45a1-8860-a15b2e7ef596", "a5cd5475-25df-4c76-b102-5428ff3414e5", "a6d6d819-2027-44cb-aaa6-7d044ccb4c0f", "a6bc7192-55a6-4efd-9060-0218331fa6e1", "a6d956a4-5506-4372-b32d-6ab534b7aac6", "a6d02fce-ae3c-4b0c-a493-464b7dbaec6f", "a6f01ed7-3cfb-4a0e-a4e7-3445c55d9410", "a7dee5c6-27d3-4899-aebb-a916bc178469", "a8bcd833-5185-412f-8004-23ad474e5cc1", "a946dc9b-da4b-4437-ac9a-980e0056448c", "a8e84e27-6337-407a-b192-8e80da31a77f", "a9d37445-7739-4af5-865e-c1151e83f11f", "ab92d7a9-4677-4090-b7f2-d26e0d5bf498", "abaf7bd5-2d48-4931-956d-ce7daf07f9b0", "ab9490a5-d44b-4c40-86dd-1f7af2c37b4f", "ac90ff7d-419c-42eb-a611-7e1d5fc9c990", "ab066c41-4820-42fc-8f14-df560bba6c74", "ab4be8ea-cc27-4add-bb06-d550d90c3571", "ac87d4e5-6d1c-466f-979a-a508420dad3b", "ad201d30-c3a8-4d00-9f54-18964bac23b4", "ad5160b7-048a-4fa2-8cae-530127781c17", "acc2c3ef-0564-40aa-91a1-468dca98f88c", "ad35627c-84e0-44d8-829c-7962ca8667e3", "add1dd85-cb5e-4d55-956c-a9193c7332b1", "af055091-a198-4090-a782-0a5e0c3a9d8e", "aff62a1d-2aba-4b96-b29a-9f1f37039bae", "af60f0a2-d9e6-40fa-92bb-7c81d525a3ad", "b019ea64-2db4-47c6-8484-c28e5ed54b7f", "b12254b7-967f-49c4-8b75-00e35890d2cb", "b0c8d344-3e79-4aaa-bc8a-5127e8acc5ff", "b15ed659-a08d-415c-9cc8-58a07da06833", "b2964338-722b-4aa8-bc02-d2267323bce1", "b379ffbf-9091-4e5d-a0ae-f8af3fd5c25b", "b449802a-7400-4a60-b9bc-b2f0ae9f9a2c", "b404f30a-df0d-4eb2-aeab-24b563206be9", "b4be4483-bfa3-4677-a102-6990335e74ef", "b59faf3c-f358-4a93-872a-3aaf0adcb937", "b5561d06-2e74-429e-900c-69de5aac1da2", "b5d7a6d6-5cbb-4048-b6c4-e6d578cc6f8a", "b66343db-177d-4b12-8929-778a59843f8b", "b5b315bb-4b54-46a4-ab9b-419d479a563e", "b6e2e660-535b-446a-8f5f-a3d79052ea84", "b4e508ed-d946-4053-9d67-2af0e6979080", "b643d510-3b30-427c-beb4-5ff490f97a3a", "b6902026-3116-465f-9179-40322c49aeca", "b706b9f4-cafb-465c-9104-1fde8705a436", "b693dbe3-edfa-4454-b3cc-d132f7517317", "b8e1edf3-d0f7-487e-9816-636723c13061", "bad7013f-ba53-4002-bb8a-cc48c9f99fc2", "ba5179e9-8457-4d38-9d04-f15d7dbb2740", "baac0d77-3b09-4297-91ed-6b46f932a1d2", "bb979792-aecf-4aa3-a0d2-5e64a8db2d68", "bc0d1d2e-eba0-40f0-bccd-64dc13db149b", "bc36f97d-c429-4df8-b999-bf4a3a065272", "bd803f34-33a7-4475-8dde-2e1c7c49e01b", "bdd85969-01c9-425e-ab9e-2c55ff8b7f8c", "bdae3b97-a801-40f0-b515-d1fcd16780c8", "bea9505f-f38b-4215-b32e-d4962c2a18b7", "beeea745-bf36-48a7-beb8-25f7d821afe7", "bf5348d2-6620-43cb-a633-6768aaf24003", "c0438d51-5756-49a6-b731-6d0ee59aeba6", "bfcfb611-ca7d-4d28-ae05-ce5337f8c05a", "c053b843-63d0-4305-8490-8029a4173275", "c09ac93b-cfd2-4144-b53d-85a3929e12d6", "c15377ba-c705-4c5e-b128-36ced205d8ce", "c18803b5-001d-465e-98f1-e341a107cc0c", "c251bd51-4fc5-4c9f-96dc-bca1a5557e82", "c2d3afb3-e1e8-47c6-a796-0a579d0dc182", "c243ef7c-8600-4e58-902e-56a54f9cce3c", "c2ae286c-6a5b-4472-bc99-8db84f2e0b8d", "c33559a1-38e6-4ff0-917d-76a45cb9d13a", "c3a72a00-6bcc-4bc6-b9e5-5b0341dccb97", "c411007d-f620-4009-98ea-8e78cb0bdd67", "c52fd694-dde5-41e5-b650-40a2a4a03ea7", "c4d79414-0619-4d7f-ba0b-c54e0177a5e8", "c52b4ffe-02f4-4518-8491-56bc64b38b02", "c7558260-11ad-4f82-a96f-0f3cfbbebec3", "c62f2e22-e0b6-4007-9956-55b13475d017", "c80d5339-6ac8-43fc-a37e-4450e25ec846", "c81ec127-10ec-42cc-901a-2563bad88b44", "c8fd8fec-4298-467c-a12f-3fb99839e33f", "c833ad9c-22d0-48fb-8ad4-3fb63cb24f5b", "c98398ba-c550-4ea6-a2a6-107422c8c3d3", "c9e1b4a5-c96e-4389-a910-225dd6dea112", "c9cb22ff-a03a-4a82-9391-698743660dbc", "caad406d-fd34-4ca7-b0cb-4bee232d25e1", "ca978277-76a6-476a-9e3b-e1af4941fa04", "cb121b2c-5de8-43f9-a137-be93f573ec21", "cb24cfc8-e4c0-4479-92cc-5e3ada8472c8", "ccc95ae5-4334-4f73-b6a7-0284faeedf07", "cd1ba1db-238a-41db-afa1-e7480d421ea7", "ce1767fe-f8cd-4ba6-a155-2e263b591af2", "ce489676-b852-4fba-90f7-c67d7b4b0ab6", "cead98bc-9b37-43a5-970f-bcb20602db83", "d0bc8871-ec5b-4916-a66b-60f3e7060912", "d0154388-3b0e-49b1-a622-c7d2fa7a9080", "d05ce930-7bad-41d3-8382-128ff629c3f6", "cff159c8-215e-4a3c-9709-8cf81d14d693", "cfdbcca7-2e55-4c80-9587-25e5093ef832", "d169fdc5-48d8-40fa-9d97-020fc4188b09", "d14195e1-3707-4040-8fed-5d1acf01e28f", "d08934fc-1265-421c-ab82-2226e6522a63", "d0f10b6c-6a83-41e5-be4e-f111d113fc25", "d24c37aa-0d83-4f60-b60c-44fe38057e66", "d35f6a3d-2439-4d5b-8c64-99dcf54dcf55", "d315d90b-74fa-44e0-a87b-db104aa57a5a", "d3b35252-f7c2-4cb1-92ba-e61395ea1857", "d3247f33-a373-4611-9722-e9083576502c", "d3b9a5ec-9ba4-4221-ac1f-9b05d39c2e11", "cf1daedb-1895-4195-9efe-078e9945d42e", "d5136282-60c8-42c0-8b64-89eb59a3568f", "d1dc5010-3146-41bb-a588-f83b32e1c599", "d55f3e85-ed10-4ff5-9a6b-93c939f77049", "d54be8fd-fa22-4279-bdb0-dca7b4532b11", "d558ecc8-4bd5-448f-9a1f-242f93fe8268", "d6167ab0-b980-42e9-8163-7d1d21bd6055", "d5edfe74-4c48-45a6-a902-08e49886dd40", "d6cb70af-67a8-460b-8720-bfae3b89b7fd", "d714a3c5-2121-4940-9c71-76a0d2bd02a5", "d744ff2f-e17a-4ca8-a818-e61ad075a38a", "d7cd6626-c334-4366-8bd0-e2b181126480", "d7aa2d2e-51fb-4420-b432-c05c877aed2f", "d6f2f08c-dcc4-485e-92c3-1a542617fb85", "d8b3ccd3-94af-44f3-ac92-1ce1933e008e", "d9a0e2f0-02b2-428b-a43d-d09617bcf2e2", "dab6c088-1426-4271-ba43-6978c4320079", "db1cdc42-f1a4-4850-b227-86fd7ad16198", "daae7477-b2de-4d7f-a1e6-e707f04dd95d", "dba07b54-46be-45d7-bf96-6842f0e01f57", "db5a34e4-739a-4c8a-a60a-63c8a5122962", "dcc73d11-3b08-4ab5-91ab-6405022c9b23", "dd18711e-7741-4009-b835-483714f206f7", "dce64170-18fd-4d86-8749-6ee955d0eddd", "ddf497b1-6208-4d50-9aaf-c300b7c211bf", "de3b71ad-46b1-4c20-9258-eb984f59545f", "de95313f-df07-4425-9bc1-1914b21236fd", "de64c28c-f2b6-41ea-be90-177dc509a726", "de92f5ae-9408-43b8-9f3b-e203a0ddbfd6", "ddfe5f7a-e2fb-4300-919f-5cd8ce08c201", "df6c5bdb-8114-4978-b634-c27378eaf0ac", "de720db4-c48e-4877-9d5e-c89c17dd5155", "df9a250e-c7c8-4608-8a60-95ad77343275", "e0e7331f-b0c5-4e4f-afad-96c14065d5c7", "e107a548-f655-490b-86a3-e606464792f3", "e124763d-c006-470c-baf8-0265535f2d6b", "e153e9b8-7314-46b0-97a1-17f63b934c8d", "e1f93346-23ba-49f7-a815-dfc131cb0973", "e1ddfb79-ae05-4f63-8efa-ea4c7d8e38b7", "e1d050d3-1a36-4b70-8b1b-2f2a140c6112", "e20584f8-75c5-4f19-a74b-45c336b6635a", "e2b5de8a-4095-4825-8485-96f1da47e0a2", "e29ab8b0-a26f-427d-8bab-0fd36c851bf4", "e2da2341-7262-436e-913e-9de23f57309b", "e3396268-8fd6-4970-861e-0f748eb02bb5", "e1d33660-9891-46ab-a805-f34f16887ed1", "e2d0f01b-9ffa-405e-b6be-b16a448cc016", "e381a22e-c47b-4052-9496-7b6875bf1289", "e469d26a-e342-45b3-a84b-060a6a9c44c1", "e4d15f9e-af13-48bd-882a-c47dc0d296db", "e2406a5b-6bf6-4c24-991e-7923c6d2ef6e", "e4c90d55-d687-49b6-a2ca-cf588d9f0f0e", "e48bc24f-c1e0-4457-9458-a8c6f94fc909", "e6a07a03-0ee5-4321-9a86-d7c5d63592dd", "e7005c08-f538-43f3-a752-33b7f6488805", "e378de5f-a3ec-4e15-b0f2-e82d3049a2af", "e8f68f47-3220-49f5-a0da-a698808f5209", "e7da3682-62f2-4a63-a6c7-64459002a4c7", "eabbc7e1-d9be-483b-8d1b-41490ec7e97a", "eac24dce-eebf-4a46-9cc1-c431e11ffa3e", "ea73d046-d94b-45ae-919f-81e4462d09fa", "ea39498e-bc3b-425b-b246-96aaf6ae59a8", "ea4f5872-fa51-42a1-9829-0f8400b55361", "ea40ab26-9495-415a-ab83-7188114ae98f", "eb9ae26e-5d6d-4461-9d88-a9398fc44b93", "eaddc1c1-9ebe-4239-b745-ed0c5194efed", "ec322cbf-3d0e-47c1-8214-d594dec5200c", "ecd21289-e111-4972-b13f-50a3ded838fc", "ec681d34-b564-4128-83bc-386e35588ea6", "ec10d04d-49d1-4d91-ab98-e3a1459871c4", "ed130121-55d9-4413-afb0-ec77b97114f5", "ed05b642-a0fa-4949-a7b3-44cbdb69c67e", "edbf1645-4c78-4638-9441-0c102589793f", "ed555a40-5a9a-4a58-af10-58e313c95ce2", "ee040f89-0472-4eb6-9911-d83f4d916328", "ee213857-5bdb-4bbd-9bfd-24b274f6544f", "ee9640b9-80a8-449c-b08d-684729f71547", "ef2bc835-45ee-496b-91d3-0b2a8678d56f", "ef502f04-1186-406a-81b8-ef3c372efc78", "ee610aac-0848-42a3-a4fd-bdaecad5b7f3", "f01943e3-89ff-4e8f-bb91-c430e156e693", "ef71336a-a9a6-4491-b904-0303d296d0e4", "f022a474-1da4-4160-be7d-2e12c5d08c96", "f04f196f-3ba5-44a1-9258-6f387e4f0f8e", "f15df739-7a55-4fb2-abd2-4224d65c9255", "f193b0f7-3a10-41ce-8049-5d1f6a18afdb", "f1a18232-3b36-45f0-b918-ae103fffbdbb", "f1cde559-f1a6-4fde-9199-ae7ce88cf3fc", "f1375068-2993-4177-8c89-fe5fa748ce0f", "f111e376-da0e-4350-adf4-e897b41d4123", "f2045673-3d22-4c2c-9ecf-dcc0d85f93e7", "f19d6f82-c4ff-47b3-82d9-a701900e5e3f", "f283559a-89a2-4d8a-a598-cad8ab8293a9", "f28b81aa-2bc0-4c0c-b2c0-977811d12541", "f2880dfb-240b-47b0-814f-75b3cce3c889", "f2b730e7-a9d1-4e8f-8f4b-0c1d4ebd1d61", "f38d0180-ec79-4e8f-96b3-833f7a120e5a", "f39e7b59-9a0f-4659-8dfe-fec73a352ed1", "f386263f-06bf-4222-859f-93c63b5acc2e", "f51c2c89-cf69-4cdd-bbd1-7321aa14433d", "f56eecde-024a-4951-9855-8ef85726a935", "f5bac663-e47b-477e-a0b8-62dadd4b1d3f", "f549cc3e-db80-4229-96d0-1842b422371c", "f63dbb44-6dd7-4337-8872-33012b0098cb", "f59c5185-a370-4b13-a205-7ba776796f33", "f5dc4d00-de24-4de8-bba7-e2700e151f1f", "f54ffdf2-4ed3-4358-b3d2-8aba788228ad", "f7747964-7dd8-4eb6-aed9-b68ca600d442", "f79a3a5e-f896-421e-aea4-3715bf333142", "f99fa9e8-15ae-4786-bf81-db1ba81e3797", "fa6fef19-9c90-406f-a681-e935fe6c96a1", "faab02d7-229a-4d4c-8d20-d896f0295e89", "fc3bf621-0b1d-4a31-a644-d199170cdfd1", "f3d4a0e0-5a8a-4d87-95c2-7c6dbbfe1caa", "fd374adb-0eaf-4f54-8098-55adad43b6ff", "fc9a0197-8c61-4865-96bf-6e468f7563c3", "fbc9c965-8492-4065-aa16-5e5caddb3095", "fd4c82b8-1df1-4a70-9ace-6f94113153fb", "fcbeab30-e03c-4c6c-b730-77a76d6e0bda", "fe14ec53-83c7-4227-a546-f1508cebd446", "fe4e8729-7206-4adc-bdae-394d7ebd3caa", "ff110f25-6713-451e-b166-6b915d71188c", "ff0c6a16-4a0b-4ac0-aee7-74968e1eac42", "ff2ed1db-24b7-4a7a-9bbb-9663883d4303", "ffe9c6a1-3f37-4c43-8fb7-1b24b20c84db",
	}
	createdAtTimestamp := int64(1734010200000)

	stmt := spanner.Statement{
		SQL: query,
		Params: map[string]interface{}{
			"livestreamIds":      livestreamIDs,
			"createdAtTimestamp": createdAtTimestamp,
		},
	}

	// Execute the query
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	// Open the output file for writing
	outputFile, err := os.Create("final_output.txt")
	if err != nil {
		log.Fatalf("Failed to create final_output.txt: %v", err)
	}
	defer outputFile.Close()
	// Write the matched results to the output file
	writer := bufio.NewWriter(outputFile)

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Failed to iterate through results: %v", err)
		}

		// Extract results
		var livestreamID string
		var durationInSec float64
		if err := row.Columns(&livestreamID, &durationInSec); err != nil {
			log.Fatalf("Failed to parse row: %v", err)
		}
		
		duration, exists := duration2Map[livestreamID]
		if exists {
			_, err := fmt.Fprintf(writer, "id: %s, total_duration_in_sec: %v, numFiles_prod_in_sec: %d, numFiles_temp_in_sec: %d\n", livestreamID, durationInSec, duration.prod, duration.temp)
			if err != nil {
				log.Fatalf("Failed to write to final_output.txt: %v", err)
			}
		}

		if err := writer.Flush(); err != nil {
			log.Fatalf("Failed to flush final_output.txt: %v", err)
		}	

	}

}
