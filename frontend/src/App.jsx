import {
  Activity,
  ArrowRight,
  BarChart3,
  BrainCircuit,
  CheckCircle2,
  ChevronRight,
  Cloud,
  Database,
  Gauge,
  GitBranch,
  Layers3,
  Radio,
  Server,
  Sparkles,
  Target,
  Trophy,
  Zap,
} from "lucide-react";
import "./App.css";

const sports = [
  {
    name: "Football",
    code: "01",
    label: "MATCH INTELLIGENCE",
    icon: "⚽",
    description:
      "Context-aware player ratings, team strength, ELO, match importance and World Cup analytics.",
    metrics: ["Player Rating", "ELO", "Match Context"],
  },
  {
    name: "Cricket",
    code: "02",
    label: "MULTI-FORMAT ANALYTICS",
    icon: "🏏",
    description:
      "Batting, bowling and player performance analytics across ODI, T20 and Test cricket.",
    metrics: ["Batting", "Bowling", "Form"],
  },
  {
    name: "Formula 1",
    code: "03",
    label: "RACE INTELLIGENCE",
    icon: "🏎️",
    description:
      "Driver performance, constructor trends, race analysis and future telemetry insights.",
    metrics: ["Drivers", "Race Pace", "Strategy"],
  },
  {
    name: "Chess",
    code: "04",
    label: "GAME INTELLIGENCE",
    icon: "♟",
    description:
      "Rating trends, player strength, openings and game-level analytical intelligence.",
    metrics: ["Rating", "Openings", "Games"],
  },
];

const intelligence = [
  {
    title: "Context Rating",
    value: "91.2",
    description: "Performance adjusted for match context",
    icon: Target,
  },
  {
    title: "Opponent Strength",
    value: "87.4",
    description: "Relative strength of the opposition",
    icon: Trophy,
  },
  {
    title: "Match Importance",
    value: "HIGH",
    description: "Competition and match significance",
    icon: Zap,
  },
  {
    title: "Consistency",
    value: "88%",
    description: "Performance stability over time",
    icon: Activity,
  },
];

const sources = [
  {
    name: "StatsBomb",
    type: "Football",
    description: "Event-level football data for deep performance analysis.",
    icon: Activity,
  },
  {
    name: "TheStatsAPI",
    type: "Football",
    description: "Structured competition, match and player statistics.",
    icon: BarChart3,
  },
  {
    name: "Cricsheet",
    type: "Cricket",
    description: "Ball-by-ball cricket data for match-level analytics.",
    icon: Database,
  },
  {
    name: "World Football Elo",
    type: "Football",
    description: "Historical team strength and international ELO ratings.",
    icon: Gauge,
  },
  {
    name: "Jolpica",
    type: "Formula 1",
    description: "Formula 1 race, driver and constructor information.",
    icon: Radio,
  },
];

const technologies = [
  ["Apache Kafka", "Streaming ingestion", Radio],
  ["Apache Spark", "Distributed processing", Sparkles],
  ["Amazon S3", "Data lake storage", Cloud],
  ["dbt", "Analytics transformation", GitBranch],
  ["FastAPI", "Backend services", Server],
  ["Athena", "Serverless analytics", Database],
];

function App() {
  return (
    <main className="app-shell">
      <nav className="navbar">
        <a className="brand" href="#">
          <div className="brand-mark">
            <Gauge size={21} />
          </div>

          <div>
            <span className="brand-title">Sports Intelligence</span>
            <span className="brand-subtitle">Data Engineering Platform</span>
          </div>
        </a>

        <div className="nav-links">
          <a href="#sports">Sports</a>
          <a href="#intelligence">Intelligence</a>
          <a href="#architecture">Architecture</a>
          <a href="#sources">Sources</a>
        </div>

        <div className="nav-status">
          <span />
          SYSTEM DEMO
        </div>
      </nav>

      {/* HERO */}

      <section className="hero">
        <div className="hero-copy">
          <div className="eyebrow">
            <span className="status-dot" />
            Multi-sport intelligence platform
          </div>

          <h1>
            Beyond the
            <span> scoreboard.</span>
          </h1>

          <p className="hero-description">
            A modern sports intelligence platform designed to transform
            multi-sport data into context-aware performance insights.
          </p>

          <div className="hero-actions">
            <a className="primary-button" href="#sports">
              Explore Platform
              <ArrowRight size={17} />
            </a>

            <a className="secondary-button" href="#architecture">
              View Architecture
            </a>
          </div>

          <div className="hero-metrics">
            <div>
              <strong>04</strong>
              <span>Sports</span>
            </div>

            <div>
              <strong>03</strong>
              <span>Data Layers</span>
            </div>

            <div>
              <strong>06+</strong>
              <span>Core Tools</span>
            </div>
          </div>
        </div>

        <div className="hero-visual">
          <div className="hero-orbit orbit-one" />
          <div className="hero-orbit orbit-two" />

          <div className="hero-dashboard">
            <div className="dashboard-top">
              <div>
                <span className="panel-label">INTELLIGENCE ENGINE</span>
                <h3>Performance Overview</h3>
              </div>

              <span className="demo-badge">
                <span />
                DEMO
              </span>
            </div>

            <div className="featured-score">
              <div>
                <span>CONTEXT RATING</span>
                <strong>91.2</strong>
              </div>

              <div className="score-ring">
                <div>
                  <span>91</span>
                  <small>/100</small>
                </div>
              </div>
            </div>

            <div className="dashboard-lines">
              <div>
                <span>Opponent Strength</span>
                <div className="dashboard-bar">
                  <i style={{ width: "87%" }} />
                </div>
                <strong>87.4</strong>
              </div>

              <div>
                <span>Consistency</span>
                <div className="dashboard-bar">
                  <i style={{ width: "88%" }} />
                </div>
                <strong>88%</strong>
              </div>

              <div>
                <span>Team Dependency</span>
                <div className="dashboard-bar">
                  <i style={{ width: "74%" }} />
                </div>
                <strong>74%</strong>
              </div>
            </div>

            <div className="dashboard-flow">
              <div>
                <Radio size={16} />
                <span>DATA</span>
              </div>
              <ChevronRight />
              <div>
                <Layers3 size={16} />
                <span>PROCESS</span>
              </div>
              <ChevronRight />
              <div>
                <BrainCircuit size={16} />
                <span>INSIGHT</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* SPORTS */}

      <section className="section" id="sports">
        <div className="section-heading">
          <div>
            <span className="section-kicker">01 / Sports Domains</span>
            <h2>One platform. Multiple sports.</h2>
          </div>

          <p>
            A common intelligence layer designed to support different sports
            without forcing every sport into the same analytical model.
          </p>
        </div>

        <div className="sports-grid">
          {sports.map((sport) => (
            <article className="sport-card" key={sport.name}>
              <div className="sport-art">
                <span>{sport.icon}</span>
                <small>{sport.code}</small>
              </div>

              <div className="sport-content">
                <span className="sport-label">{sport.label}</span>

                <h3>{sport.name}</h3>

                <p>{sport.description}</p>

                <div className="sport-metrics">
                  {sport.metrics.map((metric) => (
                    <span key={metric}>{metric}</span>
                  ))}
                </div>

                <button type="button">
                  Explore {sport.name}
                  <ArrowRight size={15} />
                </button>
              </div>
            </article>
          ))}
        </div>
      </section>

      {/* INTELLIGENCE */}

      <section className="section intelligence-section" id="intelligence">
        <div className="section-heading">
          <div>
            <span className="section-kicker">02 / Intelligence Layer</span>
            <h2>Beyond the scoreboard.</h2>
          </div>

          <p>
            The goal is not simply to display what happened. The platform is
            designed to understand the context behind performance.
          </p>
        </div>

        <div className="intelligence-layout">
          <div className="intelligence-copy">
            <div className="intelligence-number">01</div>

            <h3>Context-aware performance.</h3>

            <p>
              A player's performance should not be evaluated only through goals,
              runs, wins or trophies. Our intelligence layer considers the
              environment in which the performance happened.
            </p>

            <div className="formula">
              <span>PERFORMANCE</span>
              <strong>
                Quality × Context × Consistency
              </strong>
            </div>
          </div>

          <div className="intelligence-grid">
            {intelligence.map(({ title, value, description, icon: Icon }) => (
              <div className="intelligence-card" key={title}>
                <div className="intelligence-icon">
                  <Icon size={18} />
                </div>

                <span>{title}</span>

                <strong>{value}</strong>

                <small>{description}</small>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ARCHITECTURE */}

      <section className="section architecture-section" id="architecture">
        <div className="section-heading">
          <div>
            <span className="section-kicker">03 / Data Architecture</span>
            <h2>From raw data to intelligence.</h2>
          </div>

          <p>
            A scalable medallion-style architecture designed to separate raw
            ingestion, transformation and analytical consumption.
          </p>
        </div>

        <div className="architecture-wrapper">
          <div className="architecture-flow">
            <div className="architecture-node">
              <Radio />
              <strong>Sources</strong>
              <span>APIs · datasets · feeds</span>
            </div>

            <div className="flow-line" />

            <div className="architecture-node">
              <Radio />
              <strong>Kafka</strong>
              <span>Ingestion</span>
            </div>

            <div className="flow-line" />

            <div className="architecture-node">
              <Sparkles />
              <strong>Spark</strong>
              <span>Processing</span>
            </div>

            <div className="flow-line" />

            <div className="architecture-node featured">
              <Cloud />
              <strong>Amazon S3</strong>
              <span>Bronze · Silver · Gold</span>
            </div>

            <div className="flow-line" />

            <div className="architecture-node">
              <GitBranch />
              <strong>dbt</strong>
              <span>Data modeling</span>
            </div>

            <div className="flow-line" />

            <div className="architecture-node">
              <Server />
              <strong>FastAPI</strong>
              <span>Application API</span>
            </div>
          </div>

          <div className="architecture-caption">
            <span>
              <CheckCircle2 size={14} />
              Raw data remains preserved in the Bronze layer
            </span>

            <span>
              <CheckCircle2 size={14} />
              Analytics-ready datasets are exposed through Gold
            </span>
          </div>
        </div>
      </section>

      {/* SOURCES */}

      <section className="section" id="sources">
        <div className="section-heading">
          <div>
            <span className="section-kicker">04 / Data Sources</span>
            <h2>The data behind the platform.</h2>
          </div>

          <p>
            Source systems currently being integrated into the platform's
            multi-sport data lake.
          </p>
        </div>

        <div className="source-grid">
          {sources.map(({ name, type, description, icon: Icon }) => (
            <article className="source-card" key={name}>
              <div className="source-icon">
                <Icon size={19} />
              </div>

              <div className="source-info">
                <div>
                  <strong>{name}</strong>
                  <span>{type}</span>
                </div>

                <p>{description}</p>
              </div>

              <div className="source-arrow">
                <ArrowRight size={15} />
              </div>
            </article>
          ))}
        </div>
      </section>

      {/* TECHNOLOGY */}

      <section className="section stack-section">
        <div className="section-heading">
          <div>
            <span className="section-kicker">05 / Technology Stack</span>
            <h2>Built as an engineering system.</h2>
          </div>

          <p>
            Every technology has a defined role in the platform rather than
            simply appearing as a list of tools.
          </p>
        </div>

        <div className="tool-grid">
          {technologies.map(([name, role, Icon]) => (
            <div className="tool-card" key={name}>
              <div className="tool-icon">
                <Icon size={20} />
              </div>

              <div>
                <strong>{name}</strong>
                <span>{role}</span>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* STATUS */}

      <section className="section status-section">
        <div className="status-panel">
          <div className="status-copy">
            <span className="section-kicker">06 / Development</span>

            <h2>Building the platform in layers.</h2>

            <p>
              The first release establishes the product experience and cloud
              deployment. Real analytics will progressively replace the static
              demonstration layer.
            </p>

            <div className="status-progress">
              <div>
                <span>PROJECT PROGRESS</span>
                <strong>60%</strong>
              </div>

              <div className="progress-track">
                <i />
              </div>
            </div>
          </div>

          <div className="status-list">
            <div className="complete">
              <span>01</span>
              <div>
                <strong>Multi-sport data foundation</strong>
                <small>Completed</small>
              </div>
              <CheckCircle2 size={17} />
            </div>

            <div className="complete">
              <span>02</span>
              <div>
                <strong>AWS data lake structure</strong>
                <small>Completed</small>
              </div>
              <CheckCircle2 size={17} />
            </div>

            <div className="active">
              <span>03</span>
              <div>
                <strong>Product UI & EC2 deployment</strong>
                <small>In progress</small>
              </div>
              <Activity size={17} />
            </div>

            <div>
              <span>04</span>
              <div>
                <strong>Analytics API</strong>
                <small>Planned</small>
              </div>
              <span className="planned-dot" />
            </div>

            <div>
              <span>05</span>
              <div>
                <strong>Intelligence & ML layer</strong>
                <small>Planned</small>
              </div>
              <span className="planned-dot" />
            </div>
          </div>
        </div>
      </section>

      <footer>
        <div className="footer-brand">
          <Trophy size={17} />
          <span>Sports Intelligence Platform</span>
        </div>

        <span>
          Multi-sport analytics · Data Engineering · AWS
        </span>
      </footer>
    </main>
  );
}

export default App;