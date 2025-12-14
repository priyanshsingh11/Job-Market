import React, { useState, useEffect } from "react";
import axios from "axios";
import StartScreen from "./components/screen";

const API_BASE_URL = "http://localhost:8000";
const POWERBI_URL =
  "https://app.powerbi.com/links/R8MV_DxPfo?ctid=cb1013ae-fb33-4405-8464-8d4ebfd3679d&pbi_source=linkShare";

// Role & City options (kept from your original)
const ROLE_OPTIONS = [
  "Data Analyst",
  "Business Analyst",
  "Product Analyst",
  "Operations Analyst",
  "Marketing Analyst",
  "Finance Analyst",
  "Risk Analyst",
  "Reporting Analyst",
  "BI Analyst",
  "Analytics Engineer",
  "Data Scientist",
  "Statistician",
  "Mathematician",
  "ML Engineer",
  "AI Engineer",
  "Deep Learning Engineer",
  "Computer Vision Engineer",
  "NLP Engineer",
  "Data Engineer",
  "Data Architect",
  "Cloud Architect",
  "Solution Architect",
  "Analytics Architect",
  "Python Developer",
  "Software Engineer",
  "Data Analyst Trainee",
  "Intern",
];

const CITY_OPTIONS = [
  "Bengaluru",
  "Hyderabad",
  "Mumbai",
  "Pune",
  "Gurugram",
  "Delhi",
  "Noida",
  "Chennai",
  "Jaipur",
  "Indore",
  "Ahmedabad",
  "Lucknow",
  "Bhopal",
  "Kolkata",
  "Kochi",
  "Surat",
  "Chandigarh",
];

function Analysis({ onBack }) {
  // Try to embed the Power BI link in an iframe.
  // NOTE: PowerBI pages commonly set X-Frame-Options which prevents embedding.
  // If the iframe doesn't load or is blocked you'll want to use "Open in new tab".
  const [iframeLoaded, setIframeLoaded] = useState(false);

  useEffect(() => {
    // nothing special for now; this is a placeholder if you want to
    // implement further embed checks or analytics.
  }, []);

  return (
    <div className="analysis-root">
      <div className="analysis-header">
        <button onClick={onBack} className="small">
          ← Back
        </button>
        <h2>Power BI — Analysis Dashboard</h2>
        <div className="hint">
          If the dashboard below appears blank, Power BI is blocking embedding.
          Use "Open in new tab" instead.
        </div>
        <div className="analysis-actions">
          <button
            onClick={() => window.open(POWERBI_URL, "_blank", "noopener")}
          >
            Open in new tab
          </button>
        </div>
      </div>

      <div className="analysis-body">
        <iframe
          title="PowerBI Dashboard"
          src={POWERBI_URL}
          style={{ width: "100%", height: "80vh", border: "1px solid #ddd" }}
          onLoad={() => setIframeLoaded(true)}
        />
        {!iframeLoaded && (
          <div className="embed-fallback">
            <p>
              Embed not available (or still loading). Click{" "}
              <button
                onClick={() => window.open(POWERBI_URL, "_blank", "noopener")}
                style={{ textDecoration: "underline" }}
              >
                Open in new tab
              </button>{" "}
              to view the dashboard.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

function App() {
  const [started, setStarted] = useState(false);
  const [showAnalysis, setShowAnalysis] = useState(false);

  const [form, setForm] = useState({
    employment_type: "full-time",
    city: "Bengaluru",
    seniority: "junior",
    role: "Data Analyst",
  });

  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");

  const handleChange = (e) => {
    const { name, value } = e.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError("");
    setResult(null);

    try {
      const response = await axios.post(`${API_BASE_URL}/predict`, {
        employment_type: form.employment_type,
        city: form.city,
        seniority: form.seniority,
        role: form.role,
      });

      setResult(response.data);
    } catch (err) {
      console.error(err);
      setError(
        err.response?.data?.detail ||
          "Something went wrong while predicting. Please try again."
      );
    } finally {
      setLoading(false);
    }
  };

  const formatSalary = (salary) => {
    if (salary == null) return "-";
    return `₹ ${salary.toFixed(1)} LPA`;
  };

  const skillChips =
    result?.skills_required
      ?.split(",")
      .map((s) => s.trim())
      .filter(Boolean) || [];

   // START SCREEN with two paths: Predict OR Open Analysis (replaced by cinematic StartScreen)
      if (!started && !showAnalysis) {
        return (
          <StartScreen
            onStartApp={() => setStarted(true)}
            onOpenAnalysis={() =>
              window.open(POWERBI_URL, "_blank", "noopener")
            }
          />
        );
      }


  // MAIN APP (prediction UI)
  return (
    <div className="app-root">
      <div className="app-container">
        <header className="header">
          <h1>Job Market Intelligence</h1>
          <div style={{ marginLeft: "auto" }}>
            <button
              className="small"
              onClick={() => {
                // allow user to open analysis at any time:
                window.open(POWERBI_URL, "_blank", "noopener");
              }}
            >
              Open Analysis
            </button>
          </div>
        </header>

        <main className="main-content">
          <section className="card">
            <h2>Describe the role</h2>
            <p className="hint">
              Fill in the basic job details and we&apos;ll estimate salary and
              surface core skills.
            </p>

            <form onSubmit={handleSubmit} className="form-grid">
              <div className="form-group">
                <label htmlFor="employment_type">Employment type</label>
                <select
                  id="employment_type"
                  name="employment_type"
                  value={form.employment_type}
                  onChange={handleChange}
                  required
                >
                  <option value="full-time">Full-time</option>
                  <option value="part-time">Part-time</option>
                  <option value="contract">Contract</option>
                  <option value="internship">Internship</option>
                  <option value="freelance">Freelance</option>
                </select>
                <div className="hint">Used exactly as your model saw.</div>
              </div>

              <div className="form-group">
                <label htmlFor="city">City</label>
                <select
                  id="city"
                  name="city"
                  value={form.city}
                  onChange={handleChange}
                >
                  {CITY_OPTIONS.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
              </div>

              <div className="form-group">
                <label htmlFor="seniority">Seniority</label>
                <select
                  id="seniority"
                  name="seniority"
                  value={form.seniority}
                  onChange={handleChange}
                >
                  <option value="intern">Intern</option>
                  <option value="junior">Junior</option>
                  <option value="mid">Mid</option>
                  <option value="senior">Senior</option>
                  <option value="lead">Lead</option>
                </select>
              </div>

              <div className="form-group full-width">
                <label htmlFor="role">Role title</label>
                <select id="role" name="role" value={form.role} onChange={handleChange}>
                  {ROLE_OPTIONS.map((r) => (
                    <option key={r} value={r}>
                      {r}
                    </option>
                  ))}
                </select>
                <div className="hint">Parsed against your <code>skill_map</code>.</div>
              </div>

              <div className="form-actions">
                <button type="submit" disabled={loading}>
                  {loading ? "Crunching numbers…" : "Predict salary & skills"}
                </button>
              </div>
            </form>
          </section>

          <section className="card result-card">
            <h2>Model prediction</h2>
            <p className="hint">See how the model values this role and the expected stack.</p>

            {error && <div className="alert alert-error">{error}</div>}

            {!result && !error && (
              <div className="placeholder">
                No prediction yet. Configure the role on the left and hit{" "}
                <strong>Predict</strong>.
              </div>
            )}

            {result && (
              <div className="result-content">
                <div className="result-salary">
                  <div className="label">Estimated compensation</div>
                  <div className="value">{formatSalary(result.predicted_salary)}</div>
                </div>

                <div className="result-skills">
                  <span className="label">Core skills</span>
                  <ul>
                    {skillChips.map((skill) => (
                      <li key={skill}>{skill}</li>
                    ))}
                  </ul>
                </div>

                <div className="meta-row">
                
                </div>
              </div>
            )}
          </section>
        </main>
      </div>
    </div>
  );
}

export default App;
