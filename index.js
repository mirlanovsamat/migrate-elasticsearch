const express = require('express');
const { createClient } = require('redis');
const cors = require('cors');

const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());

const REDIS_CLIENT = createClient({
  url: 'redis://localhost:6379',
  socket: {
    connectTimeout: 10000,
    lazyConnect: true,
    keepAlive: 5000
  }
});

const SEARCH_INDEX = 'trucking_index';

const COMPANY_TYPE_MAP = {
  Broker: 'B',
  Carrier: 'C',
  Shipper: 'S',
  Registrant: 'R',
  'Freight Forwarder': 'F',
  'Intermodal Equipment Provider': 'I',
  CargoTank: 'T'
};

const AUTHORITY_MAP = {
  commonStat: 'commonStat',
  brokerStat: 'brokerStat',
  contractStat: 'contractStat'
};

const CARGO_TYPE_MAP = {
  Flatbed: 'Building Materials',
  Reefer: 'Refrigerated Food',
  'Dry van': 'General Freight',
  Tanker: 'Liquids/Gases',
  Lowboy: 'Machinery, Large Objects',
  'Step deck': 'Machinery, Large Objects',
  Conestoga: 'Machinery, Large Objects',
  'Box truck': 'Mobile Homes',
  'Intermodal container': 'Intermodal Cont',
  'Chassis trailer': 'Intermodal Cont',
  'Hopper bottom': 'Grain, Feed, Hay',
  'Livestock trailer': 'Livestock',
  'Car hauler': 'Motor Vehicles',
  Curtainside: 'Household Goods',
  'Dump trailer': 'Garbage/Refuse',
  'Extendable flatbed': 'Metal'
};

REDIS_CLIENT.connect().then(() => {
  console.log('✅ Подключение к Redis установлено');
}).catch(err => {
  console.error('❌ Ошибка подключения к Redis:', err);
});

function parseListParam(param) {
  if (!param) return [];
  if (Array.isArray(param)) {
    return param.map(item => String(item).trim()).filter(Boolean);
  }
  return String(param).split(',').map(item => item.trim()).filter(Boolean);
}

function parseNumberParam(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseBooleanParam(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const normalized = String(value).trim().toLowerCase();
  if (['true', '1', 'yes', 'on'].includes(normalized)) {
    return true;
  }
  if (['false', '0', 'no', 'off'].includes(normalized)) {
    return false;
  }
  return null;
}

function parseRangeParam(value) {
  if (!value) return null;
  const parts = String(value)
    .split('-')
    .map(part => part.trim())
    .filter(Boolean);

  const numbers = parts.map(part => Number(part)).filter(num => Number.isFinite(num));

  if (numbers.length === 0) {
    return null;
  }

  if (numbers.length === 1) {
    return { min: numbers[0], max: null };
  }

  return { min: numbers[0], max: numbers[1] };
}

function parseDateToTimestamp(value) {
  if (!value) return null;
  const parsed = Date.parse(value);
  if (!Number.isNaN(parsed)) {
    return Math.floor(parsed / 1000);
  }
  const asNumber = Number(value);
  if (!Number.isNaN(asNumber)) {
    return asNumber > 1e12 ? Math.floor(asNumber / 1000) : Math.floor(asNumber);
  }
  return null;
}

function escapeValueForRedis(value) {
  return String(value)
    .replace(/([@{}\[\]()<>:"\\|])/g, '\\$1')
    .replace(/\s+/g, '\\ ');
}

function escapeValueForSearch(value) {
  return String(value).replace(/([@{}\[\]()<>:"\\|])/g, '\\$1');
}

function buildEqualityClause(field, value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  return `@${field}:${escapeValueForRedis(value)}`;
}

function buildContainsClause(field, value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  return `@${field}:*${escapeValueForRedis(value)}*`;
}

function buildRangeClause(field, min, max) {
  const from = Number.isFinite(min) ? min : '-inf';
  const to = Number.isFinite(max) ? max : '+inf';
  return `@${field}:[${from} ${to}]`;
}

function buildMultiEqualityClause(field, values) {
  const clauses = values
    .map(value => buildEqualityClause(field, value))
    .filter(Boolean);
  if (clauses.length === 0) {
    return null;
  }
  return `(${clauses.join('|')})`;
}

function buildDotCombinations(digits) {
  const combinations = new Set();
  const padded = digits.padStart(6, '0');
  const prefixes = ['MC', 'FF', 'MX', 'WC'];

  combinations.add(`@search:*${escapeValueForRedis(digits)}*`);
  combinations.add(`@search:${escapeValueForRedis(digits)}`);

  prefixes.forEach(prefix => {
    combinations.add(`@search:${escapeValueForRedis(prefix + digits)}`);
  });

  if (digits.length < 6) {
    combinations.add(`@search:${escapeValueForRedis(padded)}`);
    prefixes.forEach(prefix => {
      combinations.add(`@search:${escapeValueForRedis(prefix + padded)}`);
    });
  }

  return [...combinations];
}

function buildSearchClause(raw) {
  const search = (raw || '').trim();
  if (!search) return null;

  const normalized = search.toUpperCase();
  const dotPattern = /(?:^|\W)(?:USDOT|DOT)\s*([0-9]+)(?:\W|$)|(?:^|\W)([0-9]+)\s*(?:USDOT|DOT)(?:\W|$)/;
  const match = normalized.match(dotPattern);
  let digits = match ? (match[1] || match[2]) : null;

  if (!digits && /^[0-9]+$/.test(normalized)) {
    digits = normalized;
  }

  if (digits) {
    const combinations = buildDotCombinations(digits);
    return `(${combinations.join('|')})`;
  }

  const lower = search.toLowerCase();
  return `@search:*${escapeValueForSearch(lower)}*`;
}

function mapCompanyType(type) {
  return COMPANY_TYPE_MAP[type];
}

function generateAllCombinations(items) {
  const result = new Set();

  function helper(current, used) {
    if (current.length === items.length) {
      result.add(current.join(';'));
      return;
    }
    for (let i = 0; i < items.length; i++) {
      if (used[i]) continue;
      used[i] = true;
      current.push(items[i]);
      helper(current, used);
      current.pop();
      used[i] = false;
    }
  }

  helper([], Array(items.length).fill(false));
  return [...result];
}

function buildCompanyTypesClause(values) {
  const mapped = values.map(mapCompanyType).filter(Boolean);
  if (mapped.length === 0) {
    return null;
  }
  const combos = generateAllCombinations(mapped);
  const clauses = combos.map(combo => `@carship:{${escapeValueForRedis(combo)}}`);
  return clauses.length ? `(${clauses.join('|')})` : null;
}

function mapCargoTypeToFlag(cargoType) {
  return CARGO_TYPE_MAP[cargoType];
}

function buildCargoCarriedClause(values) {
  const mapped = values.map(mapCargoTypeToFlag).filter(Boolean);
  if (mapped.length === 0) {
    return null;
  }
  const clauses = mapped.map(value => `@cargoCarried:*${escapeValueForRedis(value)}*`);
  return `(${clauses.join('|')})`;
}

function buildEmailClause(value) {
  if (!value) return null;
  const words = String(value).trim().split(/\s+/).filter(Boolean);
  if (!words.length) {
    return null;
  }

  const lowerWords = words.map(word => word.toLowerCase());
  const emailLike = lowerWords.find(word => word.includes('@'));
  if (emailLike) {
    return `@emailAddress:${escapeValueForRedis(emailLike)}`;
  }

  const candidate = lowerWords[0];
  if (candidate.length > 4) {
    return `@emailAddress:*${escapeValueForRedis(candidate)}*`;
  }

  return null;
}

app.get('/api/trucks', async (req, res) => {
  try {
    const { 
      page = 1, 
      limit = 20,
      sortBy,
      sortOrder = 'ASC',
      search,
      afterAddDate,
      companyTypes,
      fleetsize,
      fleetsizes,
      phyCity,
      phyState,
      phyCountry,
      insuranceCoverageMin,
      cargoTypes,
      carrierOperation,
      email,
      authority,
      bipd,
      bond,
      cargo,
      freightTypes,
      powerUnits,
      totalDrivers,
      inspectedLast24Month,
      safetyRating,
      contacts
    } = req.query;

    const sanitizedPage = Math.max(Number(page) || 1, 1);
    const sanitizedLimit = Math.min(Math.max(Number(limit) || 20, 1), 200);
    const offset = (sanitizedPage - 1) * sanitizedLimit;

    const filters = [];

    const dateClause = parseDateToTimestamp(afterAddDate);
    if (Number.isFinite(dateClause)) {
      filters.push(buildRangeClause('addDate', null, dateClause));
    }

    const searchClause = buildSearchClause(search);

    const companyTypesClause = buildCompanyTypesClause(parseListParam(companyTypes));
    if (companyTypesClause) {
      filters.push(companyTypesClause);
    }

    const fleetsizesClause = buildMultiEqualityClause('fleetsize', parseListParam(fleetsizes));
    if (fleetsizesClause) {
      filters.push(fleetsizesClause);
    }

    const fleetsizeClause = buildEqualityClause('fleetsize', fleetsize);
    if (fleetsizeClause) {
      filters.push(fleetsizeClause);
    }

    const phyCityClause = buildContainsClause('phyCity', phyCity);
    if (phyCityClause) {
      filters.push(phyCityClause);
    }

    const phyStateClause = buildEqualityClause('phyState', phyState);
    if (phyStateClause) {
      filters.push(phyStateClause);
    }

    const phyCountryClause = buildEqualityClause('phyCountry', phyCountry);
    if (phyCountryClause) {
      filters.push(phyCountryClause);
    }

    const insuranceMin = parseNumberParam(insuranceCoverageMin);
    if (insuranceMin !== null) {
      filters.push(buildRangeClause('bipdFile', insuranceMin, null));
    }

    const cargoClause = buildCargoCarriedClause(parseListParam(cargoTypes));
    if (cargoClause) {
      filters.push(cargoClause);
    }

    const freightClause = buildCargoCarriedClause(parseListParam(freightTypes));
    if (freightClause) {
      filters.push(freightClause);
    }

    const carrierOperationClause = buildEqualityClause('carrierOperation', carrierOperation);
    if (carrierOperationClause) {
      filters.push(carrierOperationClause);
    }

    const emailClause = buildEmailClause(email);
    if (emailClause) {
      filters.push(emailClause);
    }

    parseListParam(authority).forEach(type => {
      const field = AUTHORITY_MAP[type];
      if (field) {
        filters.push(buildEqualityClause(field, 'A'));
      }
    });

    const bipdRange = parseRangeParam(bipd);
    if (bipdRange) {
      filters.push(buildRangeClause('bipdFile', bipdRange.min, bipdRange.max));
    }

    const bondClause = buildEqualityClause('bondFile', bond);
    if (bondClause) {
      filters.push(bondClause);
    }

    const cargoFileClause = buildEqualityClause('cargoFile', cargo);
    if (cargoFileClause) {
      filters.push(cargoFileClause);
    }

    const powerRange = parseRangeParam(powerUnits);
    if (powerRange) {
      filters.push(buildRangeClause('powerUnits', powerRange.min, powerRange.max));
    }

    const driverRange = parseRangeParam(totalDrivers);
    if (driverRange) {
      filters.push(buildRangeClause('totalDrivers', driverRange.min, driverRange.max));
    }

    const inspectedBool = parseBooleanParam(inspectedLast24Month);
    if (inspectedBool !== null) {
      filters.push(buildEqualityClause('inspectedLast24Month', inspectedBool ? 'true' : 'false'));
    }

    const safetyClause = buildEqualityClause('safetyRating', safetyRating);
    if (safetyClause) {
      filters.push(safetyClause);
    }

    const contactsList = parseListParam(contacts);
    if (contactsList.includes('P')) {
      filters.push('@phone:*');
    }
    if (contactsList.includes('E')) {
      filters.push('@emailAddress:*');
    }

    const queryPieces = [];
    if (searchClause) {
      queryPieces.push(searchClause);
    }
    queryPieces.push(...filters);

    const finalQuery = queryPieces.length ? queryPieces.filter(Boolean).join(' ').trim() : '*';

    const searchOptions = {
      LIMIT: { from: offset, size: sanitizedLimit }
    };

    if (sortBy) {
      searchOptions.SORTBY = {
        BY: sortBy,
        DIRECTION: String(sortOrder || 'ASC').toUpperCase() === 'DESC' ? 'DESC' : 'ASC'
      };
    }

    const searchResult = await REDIS_CLIENT.ft.search(SEARCH_INDEX, finalQuery, searchOptions);

    res.json({
      success: true,
      data: {
        finalQuery,
        filters,
        page: sanitizedPage,
        limit: sanitizedLimit,
        total: searchResult.total,
        totalPages: Math.ceil(searchResult.total / sanitizedLimit),
        sortBy,
        sortOrder,
        documents: (searchResult.documents || []).map(doc => ({
          id: doc.id.replace('truck:', ''),
          ...doc.value
        }))
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 RedisSearch API сервер запущен на http://localhost:${PORT}`);
  console.log('📚 Доступные эндпоинты:');
  console.log('   GET /api/trucks - Поиск грузовиков с пагинацией и фильтрами');
});

process.on('SIGINT', async () => {
  console.log('\n🛑 Останавливаем сервер...');
  await REDIS_CLIENT.quit();
  process.exit(0);
});