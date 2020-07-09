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

  //for (const sample of samples.slice(0, 100)) {
  for (const sample of samples) {
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
  let filename;

  const comp = {
    uri: {},
    sizes: {},
    name: {},
    nickname: {},
  };

  switch (file.type) {
    case 'cram':
    case 'crai':
      filename = file.name;
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

      for (const f of filenames) {

        filename = f;
        expectedUri = 'file://' + vcfDir + '/' + filename;

        if (file.type === 'vcf' && filename.endsWith('.vcf.gz')) {
          break;
        }
        else if (file.type === 'tbi' && filename.endsWith('.vcf.gz.tbi')) {
          break;
        }
      }

      break;
  }

  let valid = true;
  let stats;
  let size;
  try {
    stats = await fs.promises.stat(file.uri.slice('file://'.length));
  }
  catch (e) {
    valid = false;
    comp.uri.act = file.uri;
    comp.uri.exp = expectedUri;
  }

  if (stats && stats.size !== Number(file.size)) {
    valid = false;
    comp.sizes.act = Number(file.size);
    comp.sizes.exp = stats.size;
    size = stats.size;
  }

  if (file.name !== filename) {
    valid = false;
    comp.name.act = file.name;
    comp.name.exp = filename;
  }

  if (file.nickname !== filename) {
    valid = false;
    comp.nickname.act = file.name;
    comp.nickname.exp = filename;
  }


  if (!valid) {

    const canRepair = await isAccessible(expectedUri);

    if (canRepair) {
      console.log("repairing");
      //console.log(file.name);
      //console.log(file);
      //console.log(comp);
      await updateFile(projectId, sampleId, file.id, expectedUri, size, filename, filename);
    }
    else {
      console.log("can't repair");
    }

    //console.log("Current URI:");
    //console.log(file.uri);
    //console.log("Expected URI:");
    //console.log(expectedUri);
  }
}

async function fileHealthy(file, uri, filename) {
}

async function updateFile(projectId, sampleId, fileId, uri, size, name, nickname) {
  const url = `${mosaicUrl}/projects/${projectId}/samples/${sampleId}/files/${fileId}`;

  const params = {
    uri,
  };

  if (size) {
    params.size = size;
  }

  if (name) {
    params.name = name;
  }

  if (nickname) {
    params.nickname = nickname;
  }

  //console.log(params);

  const res = await fetch(url, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(params),
  });

  const bodyJson = await readBody(res);
  //const body = JSON.parse(bodyJson);
  //console.log(bodyJson);
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
