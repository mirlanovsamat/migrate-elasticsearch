const { Client } = require('@elastic/elasticsearch');
const { createClient } = require('redis');

// Подключение к Elasticsearch с Basic Auth
const ES_CLIENT = new Client({ 
  node: 'http://217.77.6.58:9200',
  auth: {
    username: 'elastic',
    password: '!bw$Zq2G0*27'
  },
  requestTimeout: 120000,
  maxRetries: 5
});

const REDIS_CLIENT = createClient({
  url: 'redis://:vQK7Y6I27bS@redis-stack-vps-contabo.loadconnect.io:6379',
  socket: {
    // Дополнительные настройки для облака
    connectTimeout: 10000,
    lazyConnect: true,
    keepAlive: 5000
  }
});

const INDEX_NAME = 'trucking_index';

const INDEX_SCHEMA_FIELDS = [
  { attribute: '$.id', AS: 'id', type: 'TEXT' },
  { attribute: '$.dotNumber', AS: 'dotNumber', type: 'NUMERIC' },
  { attribute: '$.intrastateWithin100Miles', AS: 'intrastateWithin100Miles', type: 'NUMERIC' },
  { attribute: '$.totalCdl', AS: 'totalCdl', type: 'NUMERIC' },
  { attribute: '$.totalDrivers', AS: 'totalDrivers', type: 'NUMERIC' },
  { attribute: '$.avgDriversLeasedPerMonth', AS: 'avgDriversLeasedPerMonth', type: 'NUMERIC' },
  { attribute: '$.priorRevokeDotNumber', AS: 'priorRevokeDotNumber', type: 'NUMERIC' },
  { attribute: '$.addDate', AS: 'addDate', type: 'NUMERIC' },
  { attribute: '$.mcs150Mileage', AS: 'mcs150Mileage', type: 'NUMERIC' },
  { attribute: '$.mcs150MileageYear', AS: 'mcs150MileageYear', type: 'NUMERIC' },
  { attribute: '$.mcs151Mileage', AS: 'mcs151Mileage', type: 'NUMERIC' },
  { attribute: '$.totalCars', AS: 'totalCars', type: 'NUMERIC' },
  { attribute: '$.truckUnits', AS: 'truckUnits', type: 'NUMERIC' },
  { attribute: '$.powerUnits', AS: 'powerUnits', type: 'NUMERIC' },
  { attribute: '$.busUnits', AS: 'busUnits', type: 'NUMERIC' },
  { attribute: '$.mcs150Date', AS: 'mcs150Date', type: 'NUMERIC' },
  { attribute: '$.pointNum', AS: 'pointNum', type: 'NUMERIC' },
  { attribute: '$.carrierMailingCnty', AS: 'carrierMailingCnty', type: 'NUMERIC' },
  { attribute: '$.carrierMailingUndDate', AS: 'carrierMailingUndDate', type: 'NUMERIC' },
  { attribute: '$.driverInterTotal', AS: 'driverInterTotal', type: 'NUMERIC' },
  { attribute: '$.totalIntrastateDrivers', AS: 'totalIntrastateDrivers', type: 'NUMERIC' },
  { attribute: '$.reviewId', AS: 'reviewId', type: 'NUMERIC' },
  { attribute: '$.reviewDate', AS: 'reviewDate', type: 'NUMERIC' },
  { attribute: '$.mcsipDate', AS: 'mcsipDate', type: 'NUMERIC' },
  { attribute: '$.safetyRatingDate', AS: 'safetyRatingDate', type: 'NUMERIC' },
  { attribute: '$.recordableCrashRate', AS: 'recordableCrashRate', type: 'NUMERIC' },
  { attribute: '$.interstateBeyond100Miles', AS: 'interstateBeyond100Miles', type: 'NUMERIC' },
  { attribute: '$.ownTruck', AS: 'ownTruck', type: 'NUMERIC' },
  { attribute: '$.ownTract', AS: 'ownTract', type: 'NUMERIC' },
  { attribute: '$.ownTrail', AS: 'ownTrail', type: 'NUMERIC' },
  { attribute: '$.ownCoach', AS: 'ownCoach', type: 'NUMERIC' },
  { attribute: '$.ownSchool18', AS: 'ownSchool18', type: 'NUMERIC' },
  { attribute: '$.ownSchool915', AS: 'ownSchool915', type: 'NUMERIC' },
  { attribute: '$.ownSchool16', AS: 'ownSchool16', type: 'NUMERIC' },
  { attribute: '$.ownBus16', AS: 'ownBus16', type: 'NUMERIC' },
  { attribute: '$.ownVan18', AS: 'ownVan18', type: 'NUMERIC' },
  { attribute: '$.ownVan915', AS: 'ownVan915', type: 'NUMERIC' },
  { attribute: '$.ownLimo18', AS: 'ownLimo18', type: 'NUMERIC' },
  { attribute: '$.ownLimo915', AS: 'ownLimo915', type: 'NUMERIC' },
  { attribute: '$.ownLimo16', AS: 'ownLimo16', type: 'NUMERIC' },
  { attribute: '$.trmTruck', AS: 'trmTruck', type: 'NUMERIC' },
  { attribute: '$.trmTract', AS: 'trmTract', type: 'NUMERIC' },
  { attribute: '$.trmTrail', AS: 'trmTrail', type: 'NUMERIC' },
  { attribute: '$.trmCoach', AS: 'trmCoach', type: 'NUMERIC' },
  { attribute: '$.trmSchool18', AS: 'trmSchool18', type: 'NUMERIC' },
  { attribute: '$.trmSchool915', AS: 'trmSchool915', type: 'NUMERIC' },
  { attribute: '$.trmSchool16', AS: 'trmSchool16', type: 'NUMERIC' },
  { attribute: '$.trmBus16', AS: 'trmBus16', type: 'NUMERIC' },
  { attribute: '$.trmVan18', AS: 'trmVan18', type: 'NUMERIC' },
  { attribute: '$.trmVan915', AS: 'trmVan915', type: 'NUMERIC' },
  { attribute: '$.trmLimo18', AS: 'trmLimo18', type: 'NUMERIC' },
  { attribute: '$.trmLimo915', AS: 'trmLimo915', type: 'NUMERIC' },
  { attribute: '$.trmLimo16', AS: 'trmLimo16', type: 'NUMERIC' },
  { attribute: '$.trpTruck', AS: 'trpTruck', type: 'NUMERIC' },
  { attribute: '$.trpTract', AS: 'trpTract', type: 'NUMERIC' },
  { attribute: '$.trpTrail', AS: 'trpTrail', type: 'NUMERIC' },
  { attribute: '$.trpCoach', AS: 'trpCoach', type: 'NUMERIC' },
  { attribute: '$.trpSchool18', AS: 'trpSchool18', type: 'NUMERIC' },
  { attribute: '$.trpSchool915', AS: 'trpSchool915', type: 'NUMERIC' },
  { attribute: '$.trpSchool16', AS: 'trpSchool16', type: 'NUMERIC' },
  { attribute: '$.trpBus16', AS: 'trpBus16', type: 'NUMERIC' },
  { attribute: '$.trpVan18', AS: 'trpVan18', type: 'NUMERIC' },
  { attribute: '$.trpVan915', AS: 'trpVan915', type: 'NUMERIC' },
  { attribute: '$.trpLimo18', AS: 'trpLimo18', type: 'NUMERIC' },
  { attribute: '$.trpLimo915', AS: 'trpLimo915', type: 'NUMERIC' },
  { attribute: '$.trpLimo16', AS: 'trpLimo16', type: 'NUMERIC' },
  { attribute: '$.interstateWithin100Miles', AS: 'interstateWithin100Miles', type: 'NUMERIC' },
  { attribute: '$.intrastateBeyond100Miles', AS: 'intrastateBeyond100Miles', type: 'NUMERIC' },
  { attribute: '$.docket3Prefix', AS: 'docket3Prefix', type: 'TEXT' },
  { attribute: '$.docket3', AS: 'docket3', type: 'TEXT' },
  { attribute: '$.mcsipStep', AS: 'mcsipStep', type: 'TEXT' },
  { attribute: '$.hmInd', AS: 'hmInd', type: 'TEXT' },
  { attribute: '$.classDef', AS: 'classDef', type: 'TEXT' },
  { attribute: '$.phyStreet', AS: 'phyStreet', type: 'TEXT' },
  { attribute: '$.phyCity', AS: 'phyCity', type: 'TEXT' },
  { attribute: '$.phyState', AS: 'phyState', type: 'TAG' },
  { attribute: '$.phyZip', AS: 'phyZip', type: 'TEXT' },
  { attribute: '$.phyCnty', AS: 'phyCnty', type: 'TEXT' },
  { attribute: '$.carrierMailingStreet', AS: 'carrierMailingStreet', type: 'TEXT' },
  { attribute: '$.carrierMailingState', AS: 'carrierMailingState', type: 'TEXT' },
  { attribute: '$.carrierMailingCity', AS: 'carrierMailingCity', type: 'TEXT' },
  { attribute: '$.carrierMailingCountry', AS: 'carrierMailingCountry', type: 'TEXT' },
  { attribute: '$.carrierMailingZip', AS: 'carrierMailingZip', type: 'TEXT' },
  { attribute: '$.emailAddress', AS: 'emailAddress', type: 'TEXT' },
  { attribute: '$.reviewType', AS: 'reviewType', type: 'TEXT' },
  { attribute: '$.safetyRating', AS: 'safetyRating', type: 'TEXT' },
  { attribute: '$.undelivPhy', AS: 'undelivPhy', type: 'TEXT' },
  { attribute: '$.crgoCargoothrDesc', AS: 'crgoCargoothrDesc', type: 'TEXT' },
  { attribute: '$.docketNumbers[*]', AS: 'docketNumbers', type: 'TAG' },
  { attribute: '$.search[*]', AS: 'search', type: 'TEXT' },
  { attribute: '$.cargoCarried[*]', AS: 'cargoCarried', type: 'TAG' },
  { attribute: '$.phyCountry', AS: 'phyCountry', type: 'TAG' },
  { attribute: '$.docketNumber', AS: 'docketNumber', type: 'TAG' },
  { attribute: '$.mxType', AS: 'mxType', type: 'TEXT' },
  { attribute: '$.rfcNumber', AS: 'rfcNumber', type: 'TEXT' },
  { attribute: '$.commonStat', AS: 'commonStat', type: 'TEXT' },
  { attribute: '$.contractStat', AS: 'contractStat', type: 'TEXT' },
  { attribute: '$.brokerStat', AS: 'brokerStat', type: 'TEXT' },
  { attribute: '$.commonAppPend', AS: 'commonAppPend', type: 'TEXT' },
  { attribute: '$.contractAppPend', AS: 'contractAppPend', type: 'TEXT' },
  { attribute: '$.brokerAppPend', AS: 'brokerAppPend', type: 'TEXT' },
  { attribute: '$.commonRevPend', AS: 'commonRevPend', type: 'TEXT' },
  { attribute: '$.contractRevPend', AS: 'contractRevPend', type: 'TEXT' },
  { attribute: '$.brokerRevPend', AS: 'brokerRevPend', type: 'TEXT' },
  { attribute: '$.propertyChk', AS: 'propertyChk', type: 'TEXT' },
  { attribute: '$.passengerChk', AS: 'passengerChk', type: 'TEXT' },
  { attribute: '$.hhgChk', AS: 'hhgChk', type: 'TEXT' },
  { attribute: '$.privateAuthChk', AS: 'privateAuthChk', type: 'TEXT' },
  { attribute: '$.enterpriseChk', AS: 'enterpriseChk', type: 'TEXT' },
  { attribute: '$.minCovAmount', AS: 'minCovAmount', type: 'TEXT' },
  { attribute: '$.cargoReq', AS: 'cargoReq', type: 'TEXT' },
  { attribute: '$.bondReq', AS: 'bondReq', type: 'TEXT' },
  { attribute: '$.bipdFile', AS: 'bipdFile', type: 'NUMERIC' },
  { attribute: '$.cargoFile', AS: 'cargoFile', type: 'TEXT' },
  { attribute: '$.undeliverableMail', AS: 'undeliverableMail', type: 'TEXT' },
  { attribute: '$.dbaName', AS: 'dbaName', type: 'TAG' },
  { attribute: '$.legalName', AS: 'legalName', type: 'TEXT' },
  { attribute: '$.busStreetPo', AS: 'busStreetPo', type: 'TEXT' },
  { attribute: '$.busColonia', AS: 'busColonia', type: 'TEXT' },
  { attribute: '$.busCity', AS: 'busCity', type: 'TEXT' },
  { attribute: '$.busStateCode', AS: 'busStateCode', type: 'TEXT' },
  { attribute: '$.busCtryCode', AS: 'busCtryCode', type: 'TEXT' },
  { attribute: '$.busZipCode', AS: 'busZipCode', type: 'TEXT' },
  { attribute: '$.busTelno', AS: 'busTelno', type: 'TEXT' },
  { attribute: '$.busFax', AS: 'busFax', type: 'TEXT' },
  { attribute: '$.mailStreetPo', AS: 'mailStreetPo', type: 'TEXT' },
  { attribute: '$.mailColonia', AS: 'mailColonia', type: 'TEXT' },
  { attribute: '$.mailCity', AS: 'mailCity', type: 'TEXT' },
  { attribute: '$.mailStateCode', AS: 'mailStateCode', type: 'TEXT' },
  { attribute: '$.mailCtryCode', AS: 'mailCtryCode', type: 'TEXT' },
  { attribute: '$.mailZipCode', AS: 'mailZipCode', type: 'TEXT' },
  { attribute: '$.mailTelno', AS: 'mailTelno', type: 'TEXT' },
  { attribute: '$.mailFax', AS: 'mailFax', type: 'TEXT' },
  { attribute: '$.statusCode', AS: 'statusCode', type: 'TEXT' },
  { attribute: '$.dunBradstreetNo', AS: 'dunBradstreetNo', type: 'TEXT' },
  { attribute: '$.phyOmcRegion', AS: 'phyOmcRegion', type: 'TEXT' },
  { attribute: '$.safetyInvTerr', AS: 'safetyInvTerr', type: 'TEXT' },
  { attribute: '$.carrierOperation', AS: 'carrierOperation', type: 'TEXT' },
  { attribute: '$.businessOrgId', AS: 'businessOrgId', type: 'TEXT' },
  { attribute: '$.mcs150UpdateCodeId', AS: 'mcs150UpdateCodeId', type: 'TEXT' },
  { attribute: '$.priorRevokeFlag', AS: 'priorRevokeFlag', type: 'TEXT' },
  { attribute: '$.phone', AS: 'phone', type: 'TEXT' },
  { attribute: '$.fax', AS: 'fax', type: 'TEXT' },
  { attribute: '$.cellPhone', AS: 'cellPhone', type: 'TEXT' },
  { attribute: '$.companyOfficer1', AS: 'companyOfficer1', type: 'TEXT' },
  { attribute: '$.companyOfficer2', AS: 'companyOfficer2', type: 'TEXT' },
  { attribute: '$.businessOrgDesc', AS: 'businessOrgDesc', type: 'TEXT' },
  { attribute: '$.fleetsize', AS: 'fleetsize', type: 'TAG' },
  { attribute: '$.mailNationalityIndicator', AS: 'mailNationalityIndicator', type: 'TEXT' },
  { attribute: '$.phyNationalityIndicator', AS: 'phyNationalityIndicator', type: 'TEXT' },
  { attribute: '$.phyBarrio', AS: 'phyBarrio', type: 'TEXT' },
  { attribute: '$.mailBarrio', AS: 'mailBarrio', type: 'TEXT' },
  { attribute: '$.carship', AS: 'carship', type: 'TAG' },
  { attribute: '$.docket1Prefix', AS: 'docket1Prefix', type: 'TEXT' },
  { attribute: '$.docket1', AS: 'docket1', type: 'TEXT' },
  { attribute: '$.docket2Prefix', AS: 'docket2Prefix', type: 'TEXT' },
  { attribute: '$.docket2', AS: 'docket2', type: 'TEXT' },
  { attribute: '$.inspectedLast24Month', AS: 'inspectedLast24Month', type: 'TAG' },
  { attribute: '$.companyCoordinates.lat', AS: 'companyCoordinates_lat', type: 'NUMERIC' },
  { attribute: '$.companyCoordinates.lng', AS: 'companyCoordinates_lng', type: 'NUMERIC' }
];

const INDEX_SCHEMA = INDEX_SCHEMA_FIELDS.reduce((acc, field) => {
  if (!field.attribute || !field.type) {
    return acc;
  }
  acc[field.attribute] = {
    type: field.type,
    ...(field.AS ? { AS: field.AS } : {})
  };
  return acc;
}, {});

const NUMERIC_FIELDS = [
  'dotNumber',
  'intrastateWithin100Miles',
  'totalCdl',
  'totalDrivers',
  'avgDriversLeasedPerMonth',
  'priorRevokeDotNumber',
  'mcs150Mileage',
  'mcs150MileageYear',
  'mcs151Mileage',
  'totalCars',
  'truckUnits',
  'powerUnits',
  'busUnits',
  'pointNum',
  'carrierMailingCnty',
  'driverInterTotal',
  'totalIntrastateDrivers',
  'reviewId',
  'recordableCrashRate',
  'interstateBeyond100Miles',
  'ownTruck',
  'ownTract',
  'ownTrail',
  'ownCoach',
  'ownSchool18',
  'ownSchool915',
  'ownSchool16',
  'ownBus16',
  'ownVan18',
  'ownVan915',
  'ownLimo18',
  'ownLimo915',
  'ownLimo16',
  'trmTruck',
  'trmTract',
  'trmTrail',
  'trmCoach',
  'trmSchool18',
  'trmSchool915',
  'trmSchool16',
  'trmBus16',
  'trmVan18',
  'trmVan915',
  'trmLimo18',
  'trmLimo915',
  'trmLimo16',
  'trpTruck',
  'trpTract',
  'trpTrail',
  'trpCoach',
  'trpSchool18',
  'trpSchool915',
  'trpSchool16',
  'trpBus16',
  'trpVan18',
  'trpVan915',
  'trpLimo18',
  'trpLimo915',
  'trpLimo16',
  'interstateWithin100Miles',
  'intrastateBeyond100Miles',
  'bipdFile'
];

const DATE_FIELDS = [
  'addDate',
  'mcs150Date',
  'carrierMailingUndDate',
  'reviewDate',
  'mcsipDate',
  'safetyRatingDate'
];

const BOOLEAN_FIELDS = ['inspectedLast24Month'];

const ARRAY_FIELDS = ['docketNumbers', 'search', 'cargoCarried'];

function toTimestamp(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    // Normalize milliseconds vs seconds
    return value > 1e12 ? Math.floor(value / 1000) : Math.floor(value);
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;

    const asNumber = Number(trimmed);
    if (!Number.isNaN(asNumber)) {
      return trimmed.length > 10 ? Math.floor(asNumber / 1000) : Math.floor(asNumber);
    }

    const parsed = Date.parse(trimmed);
    if (!Number.isNaN(parsed)) {
      return Math.floor(parsed / 1000);
    }
  }

  if (value instanceof Date) {
    return Math.floor(value.getTime() / 1000);
  }

  return null;
}

function parseBoolean(value) {
  if (typeof value === 'boolean') {
    return value;
  }

  const normalized = String(value).trim().toLowerCase();
  if (normalized === 'true' || normalized === '1' || normalized === 'yes') {
    return true;
  }
  if (normalized === 'false' || normalized === '0' || normalized === 'no') {
    return false;
  }

  return Boolean(value);
}

function ensureArray(value) {
  if (value === undefined || value === null) {
    return [];
  }

  if (Array.isArray(value)) {
    return value.filter(item => item !== undefined && item !== null);
  }

  if (value instanceof Set) {
    return [...value];
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return [];
    }

    if ((trimmed.startsWith('[') && trimmed.endsWith(']')) || (trimmed.startsWith('{') && trimmed.endsWith('}'))) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
          return parsed.filter(item => item !== undefined && item !== null);
        }
      } catch {
        // fallback to comma split
      }
    }

    return trimmed.split(',').map(item => item.trim()).filter(Boolean);
  }

  return [value];
}

function normalizeDocument(source = {}) {
  const doc = { ...source };

  NUMERIC_FIELDS.forEach(field => {
    if (doc[field] !== undefined && doc[field] !== null && doc[field] !== '') {
      const parsed = Number(doc[field]);
      if (!Number.isNaN(parsed)) {
        doc[field] = parsed;
      }
    }
  });

  DATE_FIELDS.forEach(field => {
    const timestamp = toTimestamp(doc[field]);
    if (timestamp !== null) {
      doc[field] = timestamp;
    }
  });

  BOOLEAN_FIELDS.forEach(field => {
    if (doc[field] !== undefined) {
      doc[field] = parseBoolean(doc[field]);
    }
  });

  ARRAY_FIELDS.forEach(field => {
    const normalized = ensureArray(doc[field]);
    if (normalized.length > 0) {
      doc[field] = normalized;
    } else {
      delete doc[field];
    }
  });

  if (doc.companyCoordinates && typeof doc.companyCoordinates === 'object') {
    const normalizedCoords = {};
    ['lat', 'lng'].forEach(axis => {
      if (doc.companyCoordinates[axis] !== undefined && doc.companyCoordinates[axis] !== null && doc.companyCoordinates[axis] !== '') {
        const parsed = Number(doc.companyCoordinates[axis]);
        if (!Number.isNaN(parsed)) {
          normalizedCoords[axis] = parsed;
        }
      }
    });
    doc.companyCoordinates = normalizedCoords;
  }

  return doc;
}

async function ensureIndex() {
  try {
    await REDIS_CLIENT.ft.dropIndex(INDEX_NAME, { DD: true });
    console.log(`🗑️ Индекс "${INDEX_NAME}" удалён перед пересозданием`);
  } catch (error) {
    const message = (error.message || '').toLowerCase();
    if (!message.includes('unknown index') && !message.includes('index does not exist') && !message.includes('unrecognized index')) {
      throw error;
    }
    console.log(`⚙️ Индекс "${INDEX_NAME}" отсутствовал, создаём заново...`);
  }

  await REDIS_CLIENT.ft.create(INDEX_NAME, INDEX_SCHEMA, {
    ON: 'JSON',
    PREFIX: ['truck:']
  });
  console.log(`✅ Индекс "${INDEX_NAME}" успешно создан`);
}

// Функция для проверки памяти Redis
async function checkMemory() {
  try {
    const memoryInfo = await REDIS_CLIENT.info('memory');
    const usedMemoryMatch = memoryInfo.match(/used_memory:(\d+)/);
    const usedMemory = usedMemoryMatch ? parseInt(usedMemoryMatch[1]) : 0;
    
    console.log(`💾 Текущее использование памяти: ${(usedMemory / 1024 / 1024).toFixed(2)} MB`);
    return usedMemory;
  } catch (error) {
    console.log('⚠️  Не удалось проверить память');
    return 0;
  }
}

// Функция scroll для ограниченного количества документов (100,000)
async function scrollLimitedDocuments(limit = 5000) {
  console.log(`📖 Используем Scroll API для чтения ${limit.toLocaleString()} документов...`);
  
  let allDocuments = [];
  let scrollId = null;
  const scrollTimeout = '2m';
  const batchSize = 1000;

  try {
    // Начальный запрос scroll
    let response = await ES_CLIENT.search({
      index: 'trucking_data',
      scroll: scrollTimeout,
      size: Math.min(batchSize, limit),
      body: {
        query: { match_all: {} },
        _source: true
      }
    });

    scrollId = response._scroll_id;
    
    // Обрабатываем первую партию
    allDocuments = allDocuments.concat(response.hits.hits);
    console.log(`📦 Загружено: ${response.hits.hits.length} документов`);

    // Продолжаем scroll пока есть документы и не достигли лимита
    while (response.hits.hits.length > 0 && allDocuments.length < limit) {
      response = await ES_CLIENT.scroll({
        scroll_id: scrollId,
        scroll: scrollTimeout
      });

      scrollId = response._scroll_id;
      
      if (response.hits.hits.length > 0) {
        // Добавляем только до лимита
        const remaining = limit - allDocuments.length;
        const docsToAdd = response.hits.hits.slice(0, remaining);
        allDocuments = allDocuments.concat(docsToAdd);
        
        // Прогресс каждые 10k документов
        if (allDocuments.length % 10000 === 0) {
          console.log(`📦 Загружено: ${allDocuments.length.toLocaleString()}/${limit.toLocaleString()} документов`);
        }
        
        // Если достигли лимита - выходим
        if (allDocuments.length >= limit) {
          console.log(`🎯 Достигнут лимит в ${limit.toLocaleString()} документов`);
          break;
        }
      }
    }

    console.log(`✅ Всего загружено документов: ${allDocuments.length.toLocaleString()}`);
    
    // Очищаем scroll
    if (scrollId) {
      await ES_CLIENT.clearScroll({ scroll_id: scrollId });
    }
    
    return allDocuments;
    
  } catch (error) {
    // Всегда очищаем scroll при ошибке
    if (scrollId) {
      try {
        await ES_CLIENT.clearScroll({ scroll_id: scrollId });
      } catch (e) {
        // Игнорируем ошибки очистки
      }
    }
    throw error;
  }
}

// Альтернативная версия scroll с лимитом
async function scrollLimitedDocumentsAlternative(limit = 5000) {
  console.log(`📖 Используем альтернативный Scroll API для ${limit.toLocaleString()} документов...`);
  
  const allDocuments = [];
  let response = await ES_CLIENT.search({
    index: 'trucking_data',
    scroll: '2m',
    size: 1000,
    body: {
      query: { match_all: {} },
      _source: true
    }
  });

  while (response.hits.hits.length > 0 && allDocuments.length < limit) {
    // Добавляем только до лимита
    const remaining = limit - allDocuments.length;
    const docsToAdd = response.hits.hits.slice(0, remaining);
    allDocuments.push(...docsToAdd);
    
    if (allDocuments.length % 10000 === 0) {
      console.log(`📦 Загружено: ${allDocuments.length.toLocaleString()}/${limit.toLocaleString()} документов`);
    }
    
    // Если достигли лимита - выходим
    if (allDocuments.length >= limit) {
      console.log(`🎯 Достигнут лимит в ${limit.toLocaleString()} документов`);
      break;
    }
    
    response = await ES_CLIENT.scroll({
      scroll_id: response._scroll_id,
      scroll: '2m'
    });
  }

  // Очищаем scroll
  await ES_CLIENT.clearScroll({ scroll_id: response._scroll_id });
  
  console.log(`✅ Всего загружено документов: ${allDocuments.length.toLocaleString()}`);
  return allDocuments;
}

// Оптимизированная миграция с лимитом 100,000 записей
async function migrateLimitedData(limit = 5000) {
  try {
    console.log(`🔄 Начинаем миграцию ${limit.toLocaleString()} записей...`);
    
    // Проверяем общее количество документов
    const countResponse = await ES_CLIENT.count({ index: 'trucking_data' });
    limit = countResponse.count;
    const totalDocs = countResponse.count;
    console.log(`📊 Всего документов в индексе: ${totalDocs.toLocaleString()}`);
    console.log(`🎯 Будет мигрировано: ${Math.min(limit, totalDocs).toLocaleString()}`);
    
    const startTime = Date.now();
    const initialMemory = await checkMemory();

    let allDocuments;
    try {
      // Пробуем основной метод scroll с лимитом
      allDocuments = await scrollLimitedDocuments(limit);
    } catch (scrollError) {
      console.log('⚠️  Основной scroll не сработал, пробуем альтернативный...');
      allDocuments = await scrollLimitedDocumentsAlternative(limit);
    }

    console.log(`⏳ Начинаем сохранение ${allDocuments.length.toLocaleString()} документов в Redis...`);
    
    let migratedCount = 0;
    const batchSize = 1000;
    let consecutiveErrors = 0;
    const maxErrors = 5;

    // Сохраняем документа батчами
    for (let i = 0; i < allDocuments.length; i += batchSize) {
      try {
        const batch = allDocuments.slice(i, i + batchSize);
        
        // Используем pipeline для ускорения
        const pipeline = REDIS_CLIENT.multi();
        
        for (const doc of batch) {
          const redisKey = `truck:${doc._id}`;
          const normalizedDoc = normalizeDocument(doc._source);
          pipeline.json.set(redisKey, '$', normalizedDoc);
        }
        
        await pipeline.exec();
        migratedCount += batch.length;
        
        // Прогресс каждые 10k документов
        if (migratedCount % 10000 === 0) {
          const progress = ((migratedCount / allDocuments.length) * 100).toFixed(1);
          const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
          const docsPerMinute = Math.round(migratedCount / (elapsed || 1));
          
          console.log(`💾 Сохранено: ${migratedCount.toLocaleString()}/${allDocuments.length.toLocaleString()} (${progress}%)`);
          console.log(`   ⏱️  Время: ${elapsed} мин | 🚀 Скорость: ${docsPerMinute.toLocaleString()} док/мин`);
          
          // Проверяем память каждые 20k документов
          if (migratedCount % 20000 === 0) {
            await checkMemory();
          }
        }
        
        consecutiveErrors = 0; // Сбрасываем счетчик ошибок
        
        // Небольшая пауза чтобы не перегружать системы
        if (migratedCount % 5000 === 0) {
          await new Promise(resolve => setTimeout(resolve, 50));
        }
        
      } catch (batchError) {
        consecutiveErrors++;
        console.error(`❌ Ошибка в батче ${i}-${i + batchSize}:`, batchError.message);
        
        if (consecutiveErrors >= maxErrors) {
          console.error('💥 Слишком много ошибок подряд, останавливаем миграцию');
          break;
        }
        
        // Пауза перед повторной попыткой
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }

    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000 / 60;
    const docsPerMinute = Math.round(migratedCount / (totalTime || 1));
    
    console.log('\n🎉 МИГРАЦИЯ ЗАВЕРШЕНА!');
    console.log(`📈 Результаты:`);
    console.log(`   - Мигрировано документов: ${migratedCount.toLocaleString()}`);
    console.log(`   - Общее время: ${totalTime.toFixed(1)} минут`);
    console.log(`   - Скорость: ${docsPerMinute.toLocaleString()} док/мин`);
    console.log(`   - Успешность: ${((migratedCount / allDocuments.length) * 100).toFixed(1)}%`);
    
    // Финальная проверка памяти
    const finalMemory = await checkMemory();
    if (initialMemory > 0 && finalMemory > 0) {
      const memoryUsed = finalMemory - initialMemory;
      const avgPerDoc = Math.round(memoryUsed / migratedCount);
      console.log(`   - Память использовано: ${(memoryUsed / 1024 / 1024).toFixed(2)} MB`);
      console.log(`   - Средний размер документа: ${avgPerDoc} bytes`);
      
      // Прогноз для полного набора
      const forecastFull = (avgPerDoc * totalDocs) / 1024 / 1024;
      console.log(`   - Прогноз для всех ${totalDocs.toLocaleString()} документов: ~${forecastFull.toFixed(2)} MB`);
    }
    
  } catch (error) {
    console.error('💥 Критическая ошибка миграции:', error);
  }
}

// Основная функция
async function main() {
  try {
    const MIGRATION_LIMIT = 10000; // ← ИЗМЕНИТЕ ЗДЕСЬ если нужно другое количество
    
    console.log('🚀 ЗАПУСК ОГРАНИЧЕННОЙ МИГРАЦИИ');
    console.log('📍 Источник: http://217.77.6.58:9200/trucking_data');
    console.log('🎯 Назначение: Redis Stack localhost:6379');
    console.log(`📏 Лимит: ${MIGRATION_LIMIT.toLocaleString()} записей`);
    console.log('=' .repeat(60));
    
    // Проверяем подключение к Redis
    await REDIS_CLIENT.connect();
    console.log('✅ Подключение к Redis установлено');
    await ensureIndex();
    
    // Запускаем ограниченную миграцию
    await migrateLimitedData(MIGRATION_LIMIT);
    
    console.log('=' .repeat(60));
    console.log('✅ МИГРАЦИЯ ЗАПИСЕЙ ЗАВЕРШЕНА!');
    
  } catch (error) {
    console.error('💥 Критическая ошибка:', error);
  } finally {
    await REDIS_CLIENT.quit();
    console.log('🔚 Подключение к Redis закрыто');
  }
}

// Обработка Ctrl+C для graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Останавливаем миграцию...');
  await REDIS_CLIENT.quit();
  process.exit(0);
});

// Запуск
main();