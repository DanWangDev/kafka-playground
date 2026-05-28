import React, { useState, useEffect, useRef } from 'react';
import { 
  Database, 
  Send, 
  Terminal, 
  Plus, 
  Trash2, 
  RefreshCw, 
  Layers, 
  ArrowRight, 
  Search, 
  Play, 
  Square,
  FileCode,
  Tag
} from 'lucide-react';

const API_BASE = 'http://localhost:8081/api';
const WS_BASE = 'ws://localhost:8081/api';

const PAYLOAD_TEMPLATES = {
  user_signup: {
    key: "user-928",
    value: JSON.stringify({ event: "user_signup", user_id: 928, email: "user@example.com", timestamp: new Date().toISOString() }, null, 2),
    headers: [{ key: "source", value: "web-client" }, { key: "version", value: "1.0.0" }]
  },
  payment_processed: {
    key: "order-4421",
    value: JSON.stringify({ event: "payment_processed", order_id: 4421, amount: 89.99, currency: "USD", status: "success" }, null, 2),
    headers: [{ key: "source", value: "billing-service" }]
  },
  sensor_telemetry: {
    key: "sensor-temp-01",
    value: JSON.stringify({ sensor_id: "temp-01", reading: 22.4, status: "OK", ts: Date.now() }, null, 2),
    headers: [{ key: "device-type", value: "iot-thermometer" }]
  }
};

export default function App() {
  // Cluster state
  const [metadata, setMetadata] = useState({ brokers: [], topics: [], controller_id: -1 });
  const [backendOnline, setBackendOnline] = useState(false);
  const [loadingMetadata, setLoadingMetadata] = useState(false);

  // Topic creation state
  const [newTopicName, setNewTopicName] = useState('');
  const [newTopicPartitions, setNewTopicPartitions] = useState(3);
  const [topicActionError, setTopicActionError] = useState('');

  // Producer state
  const [selectedTopic, setSelectedTopic] = useState('');
  const [producerKey, setProducerKey] = useState('');
  const [producerValue, setProducerValue] = useState('');
  const [producerHeaders, setProducerHeaders] = useState([{ key: '', value: '' }]);
  const [publishing, setPublishing] = useState(false);
  const [publishResult, setPublishResult] = useState(null);

  // Consumer state
  const [consumerGroup, setConsumerGroup] = useState('playground-group');
  const [consumerOffset, setConsumerOffset] = useState('latest');
  const [consumerActive, setConsumerActive] = useState(false);
  const [consumedMessages, setConsumedMessages] = useState([]);
  const [filterQuery, setFilterQuery] = useState('');

  const wsRef = useRef(null);
  const consoleBottomRef = useRef(null);

  // Fetch metadata on mount and poll
  useEffect(() => {
    fetchMetadata();
    const interval = setInterval(fetchMetadata, 3000);
    return () => clearInterval(interval);
  }, []);

  // Auto-scroll consumer console
  useEffect(() => {
    if (consoleBottomRef.current) {
      consoleBottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [consumedMessages]);

  const fetchMetadata = async () => {
    setLoadingMetadata(true);
    try {
      const res = await fetch(`${API_BASE}/metadata`);
      if (res.ok) {
        const data = await res.json();
        setMetadata(data);
        setBackendOnline(true);
        // Default select topic if none selected and topics exist
        if (data.topics && data.topics.length > 0 && !selectedTopic) {
          setSelectedTopic(data.topics[0].name);
        }
      } else {
        setBackendOnline(false);
      }
    } catch (e) {
      setBackendOnline(false);
    } finally {
      setLoadingMetadata(false);
    }
  };

  const handleCreateTopic = async (e) => {
    e.preventDefault();
    if (!newTopicName) return;
    setTopicActionError('');

    try {
      const res = await fetch(`${API_BASE}/topics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newTopicName,
          partitions: Number(newTopicPartitions),
          replicationFactor: 1 // forced to 1 locally due to single broker
        })
      });

      if (res.ok) {
        setNewTopicName('');
        fetchMetadata();
      } else {
        const err = await res.json();
        setTopicActionError(err.error || 'Failed to create topic');
      }
    } catch (err) {
      setTopicActionError('Server connection error');
    }
  };

  const handleDeleteTopic = async (topicName) => {
    if (!confirm(`Are you sure you want to delete topic "${topicName}"?`)) return;
    setTopicActionError('');

    try {
      const res = await fetch(`${API_BASE}/topics/${topicName}`, {
        method: 'DELETE'
      });

      if (res.ok) {
        if (selectedTopic === topicName) {
          setSelectedTopic(metadata.topics.find(t => t.name !== topicName)?.name || '');
        }
        fetchMetadata();
      } else {
        const err = await res.json();
        setTopicActionError(err.error || 'Failed to delete topic');
      }
    } catch (err) {
      setTopicActionError('Server connection error');
    }
  };

  const handlePublish = async (e) => {
    e.preventDefault();
    if (!selectedTopic || !producerValue) return;

    setPublishing(true);
    setPublishResult(null);

    // Format headers map
    const headersMap = {};
    producerHeaders.forEach(h => {
      if (h.key.trim() && h.value.trim()) {
        headersMap[h.key.trim()] = h.value.trim();
      }
    });

    try {
      const res = await fetch(`${API_BASE}/produce`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          topic: selectedTopic,
          key: producerKey,
          value: producerValue,
          headers: headersMap
        })
      });

      if (res.ok) {
        const data = await res.json();
        setPublishResult({
          success: true,
          partition: data.partition,
          offset: data.offset
        });
      } else {
        const err = await res.json();
        setPublishResult({ success: false, error: err.error });
      }
    } catch (err) {
      setPublishResult({ success: false, error: 'Connection failed' });
    } finally {
      setPublishing(false);
    }
  };

  const applyTemplate = (templateName) => {
    const template = PAYLOAD_TEMPLATES[templateName];
    if (template) {
      setProducerKey(template.key);
      setProducerValue(template.value);
      setProducerHeaders(template.headers.length > 0 ? [...template.headers] : [{ key: '', value: '' }]);
    }
  };

  const addHeaderField = () => {
    setProducerHeaders([...producerHeaders, { key: '', value: '' }]);
  };

  const removeHeaderField = (index) => {
    const updated = [...producerHeaders];
    updated.splice(index, 1);
    setProducerHeaders(updated.length > 0 ? updated : [{ key: '', value: '' }]);
  };

  const handleHeaderChange = (index, field, val) => {
    const updated = [...producerHeaders];
    updated[index][field] = val;
    setProducerHeaders(updated);
  };

  const toggleConsumer = () => {
    if (consumerActive) {
      if (wsRef.current) {
        wsRef.current.close();
      }
      setConsumerActive(false);
    } else {
      if (!selectedTopic) return;
      setConsumedMessages([]);
      setConsumerActive(true);

      const wsUrl = `${WS_BASE}/consume/ws?topic=${selectedTopic}&groupId=${consumerGroup}&offset=${consumerOffset}`;
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          setConsumedMessages(prev => [...prev.slice(-300), msg]); // Cap at 300 messages to save RAM
        } catch (e) {
          console.error("Failed to parse websocket message", e);
        }
      };

      ws.onclose = () => {
        setConsumerActive(false);
      };

      ws.onerror = (err) => {
        console.error("WebSocket encountered error", err);
        setConsumerActive(false);
      };
    }
  };

  // Cleanup websocket on unmount
  useEffect(() => {
    return () => {
      if (wsRef.current) wsRef.current.close();
    };
  }, []);

  const filteredMessages = consumedMessages.filter(msg => {
    if (!filterQuery) return true;
    const query = filterQuery.toLowerCase();
    return (
      msg.key.toLowerCase().includes(query) ||
      msg.value.toLowerCase().includes(query) ||
      String(msg.partition).includes(query) ||
      String(msg.offset).includes(query)
    );
  });

  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
      {/* App Header */}
      <header className="app-header">
        <div className="brand-section">
          <Database size={24} className="brand-icon" />
          <span className="brand-title">Kafka Go Playground</span>
          <span className="brand-badge">KRaft Mode</span>
        </div>

        <div className="status-badge">
          <span className={`status-indicator ${backendOnline ? 'online' : 'offline'}`}></span>
          <span>{backendOnline ? 'Cluster Connected' : 'Cluster Offline'}</span>
        </div>
      </header>

      {/* Main Grid Content */}
      <main className="dashboard-container">
        {/* Left Column: Topics Admin, Metadata Info, Producer */}
        <section className="left-panel">
          {/* Topic Admin Card */}
          <div className="glass-card">
            <h2 className="card-title"><Layers size={18} /> Topic Admin</h2>
            <form onSubmit={handleCreateTopic} style={{ marginBottom: '1.25rem' }}>
              <div className="form-group">
                <label className="form-label">Topic Name</label>
                <input 
                  type="text" 
                  className="form-input" 
                  placeholder="e.g. my-events" 
                  value={newTopicName}
                  onChange={(e) => setNewTopicName(e.target.value)}
                />
              </div>
              <div className="form-group">
                <label className="form-label">Partitions</label>
                <input 
                  type="number" 
                  min="1" 
                  max="12" 
                  className="form-input" 
                  value={newTopicPartitions}
                  onChange={(e) => setNewTopicPartitions(e.target.value)}
                />
              </div>
              {topicActionError && <p style={{ color: 'var(--danger)', fontSize: '0.8rem', marginBottom: '0.75rem' }}>{topicActionError}</p>}
              <button type="submit" className="btn btn-primary" disabled={!backendOnline}>
                <Plus size={16} /> Create Topic
              </button>
            </form>

            {/* List of active topics */}
            <div style={{ borderTop: '1px solid var(--border-color)', paddingTop: '1rem' }}>
              <h3 style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginBottom: '0.75rem' }}>Active Topics ({metadata.topics.length})</h3>
              {metadata.topics.length === 0 ? (
                <p style={{ color: 'var(--text-dark)', fontSize: '0.85rem' }}>No custom topics created yet.</p>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                  {metadata.topics.map(t => (
                    <div key={t.name} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.5rem', background: 'rgba(255,255,255,0.02)', borderRadius: '4px', border: '1px solid var(--border-color)' }}>
                      <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.85rem' }}>{t.name}</span>
                      <button 
                        onClick={() => handleDeleteTopic(t.name)} 
                        className="btn btn-outline" 
                        style={{ width: 'auto', padding: '0.25rem 0.5rem', color: 'var(--danger)', borderColor: 'rgba(239, 68, 68, 0.2)' }}
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Producer Studio Card */}
          <div className="glass-card">
            <h2 className="card-title"><Send size={18} /> Producer Studio</h2>
            <form onSubmit={handlePublish}>
              <div className="form-group">
                <label className="form-label">Destination Topic</label>
                <select 
                  className="form-select" 
                  value={selectedTopic}
                  onChange={(e) => setSelectedTopic(e.target.value)}
                >
                  <option value="">-- Select a Topic --</option>
                  {metadata.topics.map(t => (
                    <option key={t.name} value={t.name}>{t.name}</option>
                  ))}
                </select>
              </div>

              {/* Templates */}
              <div style={{ display: 'flex', gap: '0.4rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
                <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)', alignSelf: 'center' }}>Templates:</span>
                <button type="button" onClick={() => applyTemplate('user_signup')} className="btn btn-outline" style={{ width: 'auto', padding: '0.25rem 0.5rem', fontSize: '0.75rem' }}>Signup</button>
                <button type="button" onClick={() => applyTemplate('payment_processed')} className="btn btn-outline" style={{ width: 'auto', padding: '0.25rem 0.5rem', fontSize: '0.75rem' }}>Payment</button>
                <button type="button" onClick={() => applyTemplate('sensor_telemetry')} className="btn btn-outline" style={{ width: 'auto', padding: '0.25rem 0.5rem', fontSize: '0.75rem' }}>IoT Sensor</button>
              </div>

              <div className="form-group">
                <label className="form-label">Message Key <span style={{ textTransform: 'none', color: 'var(--warning)' }}>(Determines Partition)</span></label>
                <input 
                  type="text" 
                  className="form-input" 
                  placeholder="e.g. order-100" 
                  value={producerKey}
                  onChange={(e) => setProducerKey(e.target.value)}
                />
              </div>

              <div className="form-group">
                <label className="form-label">Payload Value (String or JSON)</label>
                <textarea 
                  className="form-input" 
                  rows="4" 
                  style={{ fontFamily: 'var(--font-mono)', fontSize: '0.8rem' }}
                  placeholder='{"event": "test"}'
                  value={producerValue}
                  onChange={(e) => setProducerValue(e.target.value)}
                />
              </div>

              {/* Headers config */}
              <div className="form-group">
                <label className="form-label" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  Headers
                  <button type="button" onClick={addHeaderField} style={{ background: 'none', border: 'none', color: 'var(--primary)', cursor: 'pointer', fontSize: '0.75rem', fontWeight: 600 }}>+ Add Header</button>
                </label>
                {producerHeaders.map((hdr, i) => (
                  <div className="headers-row" key={i}>
                    <input 
                      type="text" 
                      placeholder="Key" 
                      className="form-input" 
                      style={{ fontSize: '0.75rem', padding: '0.4rem' }}
                      value={hdr.key}
                      onChange={(e) => handleHeaderChange(i, 'key', e.target.value)}
                    />
                    <input 
                      type="text" 
                      placeholder="Value" 
                      className="form-input" 
                      style={{ fontSize: '0.75rem', padding: '0.4rem' }}
                      value={hdr.value}
                      onChange={(e) => handleHeaderChange(i, 'value', e.target.value)}
                    />
                    <button type="button" onClick={() => removeHeaderField(i)} className="btn btn-outline" style={{ width: 'auto', padding: '0.4rem' }}>
                      <Trash2 size={12} />
                    </button>
                  </div>
                ))}
              </div>

              <button 
                type="submit" 
                className="btn btn-primary" 
                disabled={publishing || !selectedTopic || !producerValue || !backendOnline}
              >
                <Send size={16} /> {publishing ? 'Publishing...' : 'Send Message'}
              </button>

              {publishResult && (
                <div style={{ 
                  marginTop: '1rem', 
                  padding: '0.75rem', 
                  borderRadius: '6px', 
                  border: '1px solid',
                  fontSize: '0.85rem',
                  borderColor: publishResult.success ? 'rgba(16,185,129,0.3)' : 'rgba(239,68,68,0.3)',
                  background: publishResult.success ? 'var(--success-glow)' : 'var(--danger-glow)',
                }}>
                  {publishResult.success ? (
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                      <span>Successfully Published!</span>
                      <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600 }}>
                        P: {publishResult.partition} | O: {publishResult.offset}
                      </span>
                    </div>
                  ) : (
                    <div>Error: {publishResult.error}</div>
                  )}
                </div>
              )}
            </form>
          </div>
        </section>

        {/* Right Column: Cluster Visualizer & Consumer Terminal */}
        <section className="right-panel">
          {/* Topology Visualizer Card */}
          <div className="glass-card">
            <h2 className="card-title" style={{ justifyContent: 'space-between' }}>
              <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}><Layers size={18} /> Topology Visualizer</span>
              <button 
                onClick={fetchMetadata} 
                className="btn btn-outline" 
                style={{ width: 'auto', padding: '0.25rem 0.5rem' }}
                disabled={loadingMetadata}
              >
                <RefreshCw size={12} className={loadingMetadata ? 'spin-anim' : ''} />
              </button>
            </h2>

            <div className="topology-grid">
              {/* Brokers block */}
              <div>
                <h4 style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '0.5rem', textTransform: 'uppercase' }}>Brokers</h4>
                <div className="broker-list">
                  {metadata.brokers.map(b => (
                    <div 
                      key={b.id} 
                      className={`broker-card ${b.id === metadata.controller_id ? 'controller' : ''}`}
                    >
                      <Database size={20} style={{ color: 'var(--primary)', marginBottom: '0.25rem' }} />
                      <div className="broker-id">Broker #{b.id}</div>
                      <div style={{ fontSize: '0.65rem', color: 'var(--text-muted)' }}>{b.host}:{b.port}</div>
                      {b.id === metadata.controller_id && (
                        <div style={{ fontSize: '0.6rem', color: '#fff', background: 'var(--primary)', padding: '0.05rem 0.25rem', borderRadius: '3px', marginTop: '0.25rem', display: 'inline-block' }}>KRaft Controller</div>
                      )}
                    </div>
                  ))}
                  {metadata.brokers.length === 0 && (
                    <div style={{ color: 'var(--text-dark)', fontSize: '0.8rem', textAlign: 'center', padding: '1rem' }}>No active brokers</div>
                  )}
                </div>
              </div>

              {/* Topics / Partitions block */}
              <div>
                <h4 style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '0.5rem', textTransform: 'uppercase' }}>Topic Partitions</h4>
                <div className="topic-list">
                  {metadata.topics.map(t => (
                    <div className="topic-card" key={t.name}>
                      <div className="topic-meta">
                        <span className="topic-name">{t.name}</span>
                        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>{t.partitions.length} Partition(s)</span>
                      </div>
                      <div className="partition-container">
                        {t.partitions.map(p => (
                          <div className="partition-badge" key={p.id}>
                            <span className="partition-badge-title">Partition {p.id}</span>
                            <span className="partition-badge-details">Leader: Broker {p.leader}</span>
                            <span className="partition-badge-details">ISR: [{p.isr.join(',')}]</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                  {metadata.topics.length === 0 && (
                    <div style={{ color: 'var(--text-dark)', fontSize: '0.85rem', padding: '1rem', background: 'rgba(255,255,255,0.01)', border: '1px dashed var(--border-color)', borderRadius: '6px', textAlign: 'center' }}>
                      Create a topic in the Admin panel to visualize its partitions.
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* Consumer Console Card */}
          <div className="glass-card" style={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
            <h2 className="card-title"><Terminal size={18} /> Consumer Console</h2>
            
            {/* Control Bar */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.75rem', marginBottom: '1rem' }}>
              <div style={{ flex: '1 1 200px' }}>
                <label className="form-label">Topic</label>
                <select 
                  className="form-select" 
                  value={selectedTopic}
                  onChange={(e) => setSelectedTopic(e.target.value)}
                  disabled={consumerActive}
                >
                  <option value="">-- Select a Topic --</option>
                  {metadata.topics.map(t => (
                    <option key={t.name} value={t.name}>{t.name}</option>
                  ))}
                </select>
              </div>

              <div style={{ flex: '1 1 150px' }}>
                <label className="form-label">Consumer Group ID</label>
                <input 
                  type="text" 
                  className="form-input" 
                  value={consumerGroup}
                  onChange={(e) => setConsumerGroup(e.target.value)}
                  disabled={consumerActive}
                />
              </div>

              <div style={{ flex: '1 1 120px' }}>
                <label className="form-label">Auto-Offset Reset</label>
                <select 
                  className="form-select" 
                  value={consumerOffset}
                  onChange={(e) => setConsumerOffset(e.target.value)}
                  disabled={consumerActive}
                >
                  <option value="latest">Latest</option>
                  <option value="earliest">Earliest</option>
                </select>
              </div>

              <div style={{ display: 'flex', alignItems: 'flex-end', flex: '0 0 160px' }}>
                <button 
                  onClick={toggleConsumer} 
                  className={`btn ${consumerActive ? 'btn-danger' : 'btn-success'}`}
                  disabled={!selectedTopic || !backendOnline}
                >
                  {consumerActive ? (
                    <>
                      <Square size={14} /> Stop Consumer
                    </>
                  ) : (
                    <>
                      <Play size={14} /> Start Consumer
                    </>
                  )}
                </button>
              </div>
            </div>

            {/* Filter Bar */}
            {consumerActive && (
              <div style={{ position: 'relative', marginBottom: '1rem' }}>
                <Search size={16} style={{ position: 'absolute', left: '10px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
                <input 
                  type="text" 
                  className="form-input" 
                  placeholder="Filter records by key, partition, or contents..."
                  style={{ paddingLeft: '2.25rem' }}
                  value={filterQuery}
                  onChange={(e) => setFilterQuery(e.target.value)}
                />
              </div>
            )}

            {/* Real-time Console Box */}
            <div className="console-box">
              {filteredMessages.map((msg, idx) => (
                <div className="console-message" key={idx} style={{ borderLeftColor: msg.partition % 3 === 0 ? 'var(--primary)' : msg.partition % 3 === 1 ? 'var(--success)' : 'var(--warning)' }}>
                  <div className="message-meta">
                    <span className="message-badge" style={{ color: '#fff', background: msg.partition % 3 === 0 ? 'var(--primary-glow)' : msg.partition % 3 === 1 ? 'var(--success-glow)' : 'rgba(245,158,11,0.1)' }}>Partition {msg.partition}</span>
                    <span>Offset: <strong style={{ color: '#fff' }}>{msg.offset}</strong></span>
                    <span>{new Date(msg.timestamp).toLocaleTimeString()}</span>
                    {msg.key && (
                      <span>Key: <strong className="message-key">{msg.key}</strong></span>
                    )}
                  </div>
                  <div className="message-val">{msg.value}</div>
                  {Object.keys(msg.headers).length > 0 && (
                    <div style={{ display: 'flex', gap: '0.4rem', flexWrap: 'wrap', marginTop: '0.4rem' }}>
                      {Object.entries(msg.headers).map(([k, v]) => (
                        <span key={k} style={{ display: 'inline-flex', alignItems: 'center', gap: '0.2rem', fontSize: '0.65rem', color: 'var(--text-muted)', background: 'rgba(255,255,255,0.03)', padding: '0.1rem 0.3rem', borderRadius: '3px', border: '1px solid var(--border-color)' }}>
                          <Tag size={10} /> {k}: {v}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              ))}

              {filteredMessages.length === 0 && (
                <div className="console-empty">
                  <Terminal size={32} />
                  {consumerActive ? (
                    <>
                      <p>Listening to topic "{selectedTopic}"...</p>
                      <p style={{ fontSize: '0.75rem', width: '80%' }}>Send messages via the Producer Studio in another partition. They will appear here in real-time.</p>
                      <span className="status-indicator online"></span>
                    </>
                  ) : (
                    <>
                      <p>Console Idle</p>
                      <p style={{ fontSize: '0.75rem' }}>Select a topic and start the consumer to begin streaming messages.</p>
                    </>
                  )}
                </div>
              )}
              <div ref={consoleBottomRef} />
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}
