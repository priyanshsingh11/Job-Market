import React, { useState } from "react";
import axios from "axios";

const API_BASE_URL = "http://localhost:8000";

// Role dropdown values (aligned with your skill_map)
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

// City dropdown values (based on your tier logic)
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

function App() {
  // control whether main app is open or not
  const [started, setStarted] = useState(false);

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
    setForm((prev) => ({
      ...prev,
      [name]: value,
    }));
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

  /* ===========================
   * PHASE 1 – START SCREEN
   * =========================== */
  if (!started) {
    return (
      <div className="start-screen">
        <div className="start-orb start-orb-1" />
        <div className="start-orb start-orb-2" />

        <div className="start-card">
          <div className="start-chip">Job Market</div>
          <h1 className="start-title">Play with salaries, not guesses.</h1>
          <p className="start-sub">
            Launch your ML-powered salary &amp; skills explorer. See how role,
            city and seniority change your market value.
          </p>

          <button className="start-button" onClick={() => setStarted(true)}>
            Start the App
          </button>

        
        </div>
      </div>
    );
  }

  /* ===========================
   * PHASE 2 – MAIN APP
   * =========================== */
  return (
    <div className="app-root">
      <div className="app-container">
        {/* HEADER */}
        <header className="header">
          <h1>Job Market Intelligence</h1>
          
        </header>

        {/* MAIN GRID */}
        <main className="main-content">
          {/* LEFT: FORM CARD */}
          <section className="card">
            <h2>Describe the role</h2>
            <p className="hint">
              Fill in the basic job details and we&apos;ll estimate the salary
              and surface the core skills your model expects.
            </p>

            <form onSubmit={handleSubmit} className="form-grid">
              {/* Employment type */}
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
                <div className="hint">
                  Used exactly as your model saw during training.
                </div>
              </div>

              {/* City */}
              <div className="form-group">
                <label htmlFor="city">City</label>
                <select
                  id="city"
                  name="city"
                  value={form.city}
                  onChange={handleChange}
                  required
                >
                  {CITY_OPTIONS.map((city) => (
                    <option key={city} value={city}>
                      {city}
                    </option>
                  ))}
                </select>
                <div className="hint">
                  Impacts the city tier feature (Tier-1, Tier-2, Tier-3).
                </div>
              </div>

              {/* Seniority */}
              <div className="form-group">
                <label htmlFor="seniority">Seniority</label>
                <select
                  id="seniority"
                  name="seniority"
                  value={form.seniority}
                  onChange={handleChange}
                  required
                >
                  <option value="intern">Intern</option>
                  <option value="junior">Junior</option>
                  <option value="mid">Mid</option>
                  <option value="senior">Senior</option>
                  <option value="lead">Lead</option>
                </select>
              </div>

              {/* Role */}
              <div className="form-group full-width">
                <label htmlFor="role">Role title</label>
                <select
                  id="role"
                  name="role"
                  value={form.role}
                  onChange={handleChange}
                  required
                >
                  {ROLE_OPTIONS.map((role) => (
                    <option key={role} value={role}>
                      {role}
                    </option>
                  ))}
                </select>
                <div className="hint">
                  Parsed against your <code>skill_map</code> to infer skills.
                </div>
              </div>

              {/* Button row */}
              <div className="form-actions">
                <button type="submit" disabled={loading}>
                  {loading ? "Crunching numbers…" : "Predict salary & skills"}
                </button>
              </div>
            </form>
          </section>

          {/* RIGHT: RESULT CARD */}
          <section className="card result-card">
            <h2>Model prediction</h2>
            <p className="hint">
              See how the model values this role in the chosen city and what
              tech stack it expects.
            </p>

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
                  <div className="value">
                    {formatSalary(result.predicted_salary)}
                  </div>
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
                  <div className="meta-pill">
                    <span className="label">Role</span>
                    <span className="value">{form.role}</span>
                  </div>
                  <div className="meta-pill">
                    <span className="label">City</span>
                    <span className="value">{form.city}</span>
                  </div>
                  <div className="meta-pill">
                    <span className="label">Seniority</span>
                    <span className="value">{form.seniority}</span>
                  </div>
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
