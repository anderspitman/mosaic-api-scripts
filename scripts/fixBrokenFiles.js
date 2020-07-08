const http = require('https');
const fs = require('fs');


if (process.argv.length < 5) {
  console.error("Invalid arguments");
  process.exit(1);
}

const token = process.argv[2];
const projectId = process.argv[3];
const projectDir = process.argv[4];

const mosaicUrl = 'https://mosaic.chpc.utah.edu/api/v1';

const dataDir = `${projectDir}/Data/PolishedBams`;
const vcfDir = `${projectDir}/VCF/Complete`;

(async () => {

  const url = `${mosaicUrl}/projects/${projectId}/samples`;
  const res = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${token}`,
    },
  });

  const body = await readBody(res);

  const samples = JSON.parse(body);

  for (const sample of samples.slice(0, 3)) {
  //for (const sample of samples) {
    //console.log(sample);
    const files = await getSampleFiles(projectId, sample.id);
    await checkFiles(projectId, sample.id, files);
  }

})();


async function checkFiles(projectId, sampleId, files) {
  for (const file of files) {
    await checkFile(projectId, sampleId, file);    
  }
}

async function checkFile(projectId, sampleId, file) {
  //console.log(file);

  let expectedUri;

  switch (file.type) {
    case 'cram':
    case 'crai':
      expectedUri = `file://${dataDir}/${file.name}`;
      break;
    case 'vcf':
    case 'tbi':
      let filenames;
      try {
        filenames = await fs.promises.readdir(vcfDir);
      }
      catch (e) {
        console.error(e);
      }

      let vcfPath;
      let tbiPath;

      for (const filename of filenames) {
        if (filename.endsWith('.vcf.gz')) {
          vcfPath = vcfDir + '/' + filename;
        }
        else if (filename.endsWith('.vcf.gz.tbi')) {
          tbiPath = vcfDir + '/' + filename;
        }
      }

      if (file.type === 'vcf') {
        expectedUri = 'file://' + vcfPath;
      }
      else {
        expectedUri = 'file://' + tbiPath;
      }

      break;
  }

  const broken = !await isAccessible(file.uri);

  if (broken) {

    console.log(expectedUri);

    const canRepair = await isAccessible(expectedUri);

    if (canRepair) {
      console.log("repairing");
      await updateFileUri(projectId, sampleId, file.id, expectedUri);
    }
    else {
      console.log("can't repair");
    }

    console.log("Current URI:");
    console.log(file.uri);
    console.log("Expected URI:");
    console.log(expectedUri);
  }
}

async function updateFileUri(projectId, sampleId, fileId, uri) {
  const url = `${mosaicUrl}/projects/${projectId}/samples/${sampleId}/files/${fileId}`;
  console.log(url);
  const res = await fetch(url, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      uri,
    }),
  });

  const bodyJson = await readBody(res);
  //const body = JSON.parse(bodyJson);
  console.log(bodyJson);
}


function filenameToSampleName(filename) {
  return filename.split('.')[0];
}

async function getSampleFiles(projectId, sampleId) {

  const url = `${mosaicUrl}/projects/${projectId}/samples/${sampleId}/files`;

  const res = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${token}`,
    },
  });

  const body = await readBody(res);
  const files = JSON.parse(body);

  return files.data;
}

async function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
    });

    req.on('end', async () => {
      resolve(data);
    });

    req.on('error', async (err) => {
      reject(err);
    });
  });
}

async function fetch(url, options) {
  return new Promise((resolve, reject) => {

    const req = http.request(url, options, (res) => {
      resolve(res);
    });

    if (options.body) {
      req.write(options.body);
    }

    req.end();
  });
}

async function isAccessible(uri) {

  if (!uri) return false;

  const path = uri.slice('file://'.length);
  const result = await fs.promises.access(path)
    .then(() => true)
    .catch(() => false);

  return result;
}
