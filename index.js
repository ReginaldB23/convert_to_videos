const AWS = require('aws-sdk');
const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const fs = require('fs');
const moment = require('moment-timezone');


AWS.config.update({
    maxRetries: 2,
    httpOptions: {
        timeout: 3000,
        connectTimeout: 2000
    },
    region: "us-west-2"
});

// Set up the AWS SDK
const s3 = new AWS.S3();

const BUCKET_NAME = 'suiteview-storage';

const folderDate = moment().utcOffset(480).format('YYYY-MM-DD');

const main = async (event) => {
      try {
        const cameras = await getCameraFolders();
        for (const cameraName of cameras) {
          const timestamp = moment().utcOffset(480).format('YYYY-MM-DD-HH');
          const cameraImagesFolder = `${cameraName}/Images/`;
          const cameraFootageFolder = `${cameraName}/Footage/`;
          const imagesFolderExists = await checkFolderExists(cameraImagesFolder);
          if (imagesFolderExists) {
            const IMAGES_FOLDER = cameraImagesFolder + folderDate + '/';
            const FOOTAGE_FOLDER = cameraFootageFolder + folderDate + '/';
            const imageKeys = await listImageKeys(IMAGES_FOLDER);
            if (imageKeys.length === 0) {
              console.log(`No images found for camera ${cameraName}, skipping...`);
              continue;
            }
            await writeImagesToTmp(imageKeys);
            await compileVideo(FOOTAGE_FOLDER);
            const videoKey = `${FOOTAGE_FOLDER}${cameraName}-${timestamp}-24hrs.mp4`;
            await saveVideo(videoKey, cameraName, FOOTAGE_FOLDER);
            console.log(`Video saved for camera ${cameraName} ${timestamp}`);
          } else {
            console.log(`Images/ folder not found for camera ${cameraName}, skipping...`);
          }
        }
        return 'Success';
      } catch (error) {
        console.error(error);
        return 'Error';
      }

    async function getCameraFolders() {
        const params = {
            Bucket: BUCKET_NAME,
            Prefix: '',
            Delimiter: '/'
        };
        const response = await s3.listObjectsV2(params).promise();
        return response.CommonPrefixes.map(prefix => prefix.Prefix.split('/').filter(Boolean)[0]);
    }

    async function checkFolderExists(folder) {
        const params = {
            Bucket: BUCKET_NAME,
            Prefix: folder,
            Delimiter: '/'
        };
        const response = await s3.listObjectsV2(params).promise();
        return response.CommonPrefixes.length > 0;
    }

    async function listImageKeys(folder) {
      const MAX_KEYS = 1000; // maximum number of keys to retrieve per API request
      const params = {
        Bucket: BUCKET_NAME,
        Prefix: folder,
        MaxKeys: MAX_KEYS
      };
      let filteredImageKeys = [];
    
      do {
        const response = await s3.listObjectsV2(params).promise();
        const now = new Date();
        const startOfHour = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours()-24, 0, 0);
        const endOfHour = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours(), 59, 59);
    
        filteredImageKeys.push(
          ...response.Contents
            .filter(obj => obj.LastModified >= startOfHour && obj.LastModified <= endOfHour) // filter images created within the current hour
            .filter(obj => obj.Key.endsWith('.jpg')) // filter out non-image keys
            .sort((a, b) => a.LastModified - b.LastModified) // sort by LastModified timestamp
            .map(obj => obj.Key)
        );
    
        params.ContinuationToken = response.NextContinuationToken;
      } while (params.ContinuationToken);
    
      return filteredImageKeys;
    }

    async function getImage(key) {
        const params = {
            Bucket: BUCKET_NAME,
            Key: key
        };
        const response = await s3.getObject(params).promise();
        return response.Body;
    }

async function writeImagesToTmp(imageKeys) {
  for (let i = 0; i < imageKeys.length; i++) {
    if (imageKeys[i].includes('.jpg')) {
      const image = await getImage(imageKeys[i]);
      const imagePath = path.join('/tmp', `image-${i}.jpg`);
      fs.writeFileSync(imagePath, image);
    }
  }
}

async function compileVideo() {

  const video = ffmpeg()
    .input('/tmp/image-%d.jpg')
    // Tell FFmpeg to import the frames at 10 fps
    .inputOptions('-framerate', '10')
    // Use the H.264 video codec
    .videoCodec('libvpx-vp9')
    // Use YUV color space with 4:2:0 chroma subsampling for maximum compatibility with
    // video players
    .outputOptions('-pix_fmt', 'yuv420p')
    // Set the video bitrate to 10 Mbps
    .outputOptions('-b:v', '2M')
    // Set the video metadata to indicate the content type
    .outputOptions('-metadata', `ContentType=video/mp4`)
    // Use the fragmented MP4 format for streaming
    .outputOptions(['-movflags', 'frag_keyframe+empty_moov'])
    .toFormat('mp4')
    .saveToFile('/tmp/video.mp4');

  await new Promise((resolve, reject) => {
    video.on('error', reject).on('end', resolve).run();
  });
}

async function saveVideo(videoKey, cameraName, FOOTAGE_FOLDER) {
  const videoPath = path.join('/tmp', 'video.mp4');

  const params = {
    Bucket: BUCKET_NAME,
    Key: videoKey,
    Body: fs.createReadStream(videoPath),
    ContentType: 'video/mp4',
    Tagging: `camera=${cameraName}&interval_hours=1`,
  };


  // Check if the date folder inside the footage folder exists, create it if it doesn't
  const folderParams = {
    Bucket: BUCKET_NAME,
    Prefix: FOOTAGE_FOLDER,
  };
  const folderExists = await s3
    .listObjectsV2(folderParams)
    .promise()
    .then((res) => res.Contents.length > 0);
  if (!folderExists) {
    const createFolderParams = {
      Bucket: BUCKET_NAME,
      Key: FOOTAGE_FOLDER,
      ContentType: 'video/mp4',
      Tagging: `camera=${cameraName}&interval_hours=1`,
    };
    await s3.putObject(createFolderParams).promise();
  }

  await s3.putObject(params).promise();
  
  try {
    // Get the contents of the /tmp directory
    const tmpDir = '/tmp';
    const files = fs.readdirSync(tmpDir);

    // Filter the image files
    const imageFiles = files.filter(file => /\.(jpg|jpeg|png|gif)$/i.test(file));

    // Process the image files
    imageFiles.forEach(imageFile => {
      const imagePath = path.join(tmpDir, imageFile);


      // Delete the image file
      fs.unlinkSync(imagePath);
    });

    return {
      statusCode: 200,
      body: 'Image files processed and deleted'
    };
  } catch (error) {
    console.error(`Error processing or deleting image files: ${error}`);

    return {
      statusCode: 500,
      body: 'Error processing or deleting image files'
    };
  }
}
}

exports.handler = main;