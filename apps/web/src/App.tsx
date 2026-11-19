function App() {
  return (
    <main className="shell">
      <header className="topbar">
        <div className="brand-mark" aria-hidden="true">
          W
        </div>
        <div>
          <p className="eyebrow">Wildfire monitoring</p>
          <h1>Intelligence Platform</h1>
        </div>
        <span className="environment">LOCAL ENVIRONMENT</span>
      </header>

      <section className="welcome" aria-labelledby="welcome-title">
        <p className="eyebrow">Project foundation</p>
        <h2 id="welcome-title">Situational awareness starts here.</h2>
        <p>
          The platform foundation is ready. Fire detections, weather, and
          geospatial layers will be brought together in this workspace.
        </p>
        <div className="service-list" aria-label="Foundation services">
          <span><i /> Web application</span>
          <span><i /> API service</span>
          <span><i /> PostGIS + Kafka</span>
        </div>
      </section>

      <footer>Built for wildfire monitoring and response across Southeast Asia.</footer>
    </main>
  );
}

export default App;
