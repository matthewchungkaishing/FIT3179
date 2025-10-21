// Sun & Skin — Application JavaScript
// Manages 4 Vega-Lite visualizations with cross-chart interactivity

// Month names for display
const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December'
];

// State data lookup (from uv_state_metric.csv)
const STATE_DATA = {
  'NT': { name: 'Northern Territory', annual: 11.4, peak_month: 3, peak_uvi: 13.9 },
  'QLD': { name: 'Queensland', annual: 8.1, peak_month: 1, peak_uvi: 12.9 },
  'WA': { name: 'Western Australia', annual: 7.6, peak_month: 1, peak_uvi: 12.2 },
  'NSW': { name: 'New South Wales', annual: 6.7, peak_month: 1, peak_uvi: 11.7 },
  'SA': { name: 'South Australia', annual: 6.6, peak_month: 1, peak_uvi: 11.6 },
  'ACT': { name: 'Australian Capital Territory', annual: 6.4, peak_month: 1, peak_uvi: 11.5 },
  'VIC': { name: 'Victoria', annual: 5.9, peak_month: 1, peak_uvi: 10.5 },
  'TAS': { name: 'Tasmania', annual: 5.2, peak_month: 12, peak_uvi: 10.6 }
};

// Melanoma data lookup (from melanoma_rates_state_5yr_mean.csv)
const MELANOMA_DATA = {
  'ACT': { asr: 49.14, total_cases: 1009 },
  'NSW': { asr: 53.12, total_cases: 25027 },
  'NT': { asr: 46.86, total_cases: 448 },
  'QLD': { asr: 72.7, total_cases: 20866 },
  'SA': { asr: 37.56, total_cases: 4283 },
  'TAS': { asr: 56.42, total_cases: 1975 },
  'VIC': { asr: 38.76, total_cases: 14330 },
  'WA': { asr: 51.22, total_cases: 7588 }
};

// Global references to chart views
let uvMapView = null;
let melanomaMapView = null;
let scatterView = null;
let currentSelectedState = 'NSW';
let currentMapContext = 'uv';

// Update State Card
function updateStateCard(stateCode, context = 'uv') {
  if (!stateCode || !STATE_DATA[stateCode]) {
    // Show default "—" if no state selected
    document.getElementById('card-state-name').textContent = '—';
    document.getElementById('card-annual-uvi').textContent = '—';
    document.getElementById('card-peak-month').textContent = '—';
    document.getElementById('card-peak-uvi').textContent = '—';
    document.getElementById('card-asr').textContent = '—';
    document.getElementById('card-total-cases').textContent = '—';
    return;
  }

  const state = STATE_DATA[stateCode];
  const melanoma = MELANOMA_DATA[stateCode];
  
  document.getElementById('card-state-name').textContent = state.name;
  
  if (context === 'uv') {
    document.getElementById('uv-metrics').style.display = 'grid';
    document.getElementById('melanoma-metrics').style.display = 'none';
    
    document.getElementById('card-annual-uvi').textContent = state.annual.toFixed(1);
    document.getElementById('card-peak-month').textContent = MONTH_NAMES[state.peak_month - 1];
    document.getElementById('card-peak-uvi').textContent = state.peak_uvi.toFixed(1);
  } else if (context === 'melanoma') {
    document.getElementById('uv-metrics').style.display = 'none';
    document.getElementById('melanoma-metrics').style.display = 'grid';
    
    document.getElementById('card-asr').textContent = melanoma.asr.toFixed(1);
    document.getElementById('card-total-cases').textContent = melanoma.total_cases.toLocaleString();
  }
}

// Embed Charts
async function embedCharts() {
  try {
    // Embed UV Map
    const uvMapResult = await vegaEmbed('#map-uv', 'specs/map_uv.json', {
      actions: false,
      renderer: 'svg'
    });
    uvMapView = uvMapResult.view;

    // Listen for UV map selections
    uvMapView.addSignalListener('stateSel', (name, value) => {
      const stateCodeKey = 'properties\\.state_code';
      if (value && value[stateCodeKey] && value[stateCodeKey].length > 0) {
        const selectedStateCode = value[stateCodeKey][0];
        currentSelectedState = selectedStateCode;
        currentMapContext = 'uv';
        updateStateCard(selectedStateCode, 'uv');
        
        if (scatterView) {
          scatterView.signal('selectedState', selectedStateCode).run();
        }
      } else {
        currentSelectedState = null;
        updateStateCard(null, 'uv');
        
        if (scatterView) {
          scatterView.signal('selectedState', null).run();
        }
      }
    });

    // Embed Seasonality Lines
    await vegaEmbed('#lines-uv', 'specs/lines_uv_capitals.json', {
      actions: false,
      renderer: 'svg'
    });

    // Embed Melanoma Map
    const melanomaMapResult = await vegaEmbed('#map-melanoma', 'specs/map_melanoma.json', {
      actions: false,
      renderer: 'svg'
    });
    melanomaMapView = melanomaMapResult.view;

    // Listen for Melanoma map selections
    melanomaMapView.addSignalListener('melanomaSel', (name, value) => {
      const stateCodeKey = 'properties\\.state_code';
      if (value && value[stateCodeKey] && value[stateCodeKey].length > 0) {
        const selectedStateCode = value[stateCodeKey][0];
        currentSelectedState = selectedStateCode;
        currentMapContext = 'melanoma';
        updateStateCard(selectedStateCode, 'melanoma');
        
        if (scatterView) {
          scatterView.signal('selectedState', selectedStateCode).run();
        }
      } else {
        currentSelectedState = null;
        updateStateCard(null, 'melanoma');
        
        if (scatterView) {
          scatterView.signal('selectedState', null).run();
        }
      }
    });

    // Embed Scatter Plot
    const scatterResult = await vegaEmbed('#scatter', 'specs/scatter_uv_melanoma.json', {
      actions: false,
      renderer: 'svg'
    });
    scatterView = scatterResult.view;
    
    scatterView.signal('selectedState', currentSelectedState).run();
    
  } catch (error) {
    console.error('Error embedding charts:', error);
  }
}

// Initialize on DOM Ready
document.addEventListener('DOMContentLoaded', () => {
  updateStateCard('NSW');
  embedCharts();
});

