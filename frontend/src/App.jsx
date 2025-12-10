import React, { useState } from "react";
import axios from "axios";

const API_BASE_URL = "http://localhost:8000"; // change if backend is elsewhere

function App() {
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
    // assuming your model predicts in LPA (like 3.0, 7.5 etc.)
    return `₹ ${salary.toFixed(1)} LPA`;
  };

  return (
    <div className="app-root">
      <div className="app-container">
        <header className="header">
          <h1>Job Market – Salary & Skills Predictor</h1>
          <p>
            Enter basic job details and get an estimated salary range plus core
            skills you should have.
          </p>
        </header>

        <main className="main-content">
          {/* FORM CARD */}
          <section className="card form-card">
            <h2>Job Details</h2>
            <form onSubmit={handleSubmit} className="form-grid">
              {/* Employment Type */}
              <div className="form-group">
                <label htmlFor="employment_type">Employment Type</label>
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
              </div>

              {/* City */}
              <div className="form-group">
                <label htmlFor="city">City</label>
                <input
                  id="city"
                  name="city"
                  type="text"
                  placeholder="e.g. Bengaluru"
                  value={form.city}
                  onChange={handleChange}
                  required
                />
                <small className="hint">
                  Used for city tier: Bengaluru, Hyderabad, Mumbai, Jaipur, etc.
                </small>
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
                <label htmlFor="role">Role Title</label>
                <input
                  id="role"
                  name="role"
                  type="text"
                  placeholder="e.g. Data Analyst, Analytics Engineer, ML Engineer"
                  value={form.role}
                  onChange={handleChange}
                  required
                />
                <small className="hint">
                  Your backend uses this to infer skills (via <code>skill_map</code>).
                </small>
              </div>

              <div className="form-actions">
                <button type="submit" disabled={loading}>
                  {loading ? "Predicting..." : "Predict Salary & Skills"}
                </button>
              </div>
            </form>
          </section>

          {/* RESULT CARD */}
          <section className="card result-card">
            <h2>Prediction Result</h2>

            {error && <div className="alert alert-error">{error}</div>}

            {!error && !result && (
              <p className="placeholder">
                Fill the form and click <strong>Predict</strong> to see the
                estimated salary and key skills.
              </p>
            )}

            {result && (
              <div className="result-content">
                <div className="result-salary">
                  <span className="label">Predicted Salary</span>
                  <span className="value">
                    {formatSalary(result.predicted_salary)}
                  </span>
                </div>

                <div className="result-skills">
                  <span className="label">Core Skills Required</span>
                  <ul>
                    {result.skills_required
                      .split(",")
                      .map((s) => s.trim())
                      .filter(Boolean)
                      .map((skill) => (
                        <li key={skill}>{skill}</li>
                      ))}
                  </ul>
                </div>
              </div>
            )}
          </section>
        </main>

        <footer className="footer">
          <span>Built with FastAPI + CatBoost + React</span>
        </footer>
      </div>
    </div>
  );
}

export default App;
